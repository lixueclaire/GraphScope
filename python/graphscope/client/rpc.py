#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import functools
import json
import logging
import sys
import threading
import time

import grpc

from graphscope.config import GSConfig as gs_config
from graphscope.framework.errors import ConnectionError
from graphscope.framework.errors import GRPCError
from graphscope.framework.errors import check_grpc_response
from graphscope.proto import coordinator_service_pb2_grpc
from graphscope.proto import error_codes_pb2
from graphscope.proto import message_pb2
from graphscope.version import __version__

logger = logging.getLogger("graphscope")


def catch_grpc_error(fn):
    """Print error info from a :class:`grpc.RpcError`."""

    @functools.wraps(fn)
    def with_grpc_catch(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except grpc.RpcError as exc:
            if isinstance(exc, grpc.Call):
                # pylint: disable=no-member
                raise GRPCError(
                    "rpc %s: failed with error code %s, details: %s"
                    % (fn.__name__, exc.code(), exc.details())
                ) from exc
            else:
                raise GRPCError(
                    "rpc %s failed: status %s" % (str(fn.__name__), exc)
                ) from exc

    return with_grpc_catch


def suppress_grpc_error(fn):
    """Suppress the GRPC error."""

    @functools.wraps(fn)
    def with_grpc_catch(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except grpc.RpcError as exc:
            if isinstance(exc, grpc.Call):
                logger.warning(
                    "Grpc call '%s' failed: %s: %s",
                    fn.__name__,
                    exc.code(),
                    exc.details(),
                )
        except Exception as exc:  # noqa: F841
            logger.warning("RPC call failed: %s", exc)

    return with_grpc_catch


class GRPCClient(object):
    def __init__(self, endpoint, reconnect=False):
        """Connect to GRAPE engine at the given :code:`endpoint`."""
        # create the gRPC stub
        options = [
            ("grpc.max_send_message_length", 2147483647),
            ("grpc.max_receive_message_length", 2147483647),
        ]
        self._channel = grpc.insecure_channel(endpoint, options=options)
        self._stub = coordinator_service_pb2_grpc.CoordinatorServiceStub(self._channel)
        self._session_id = None
        self._logs_fetching_thread = None
        self._reconnect = reconnect

    def waiting_service_ready(self, timeout_seconds=60):
        begin_time = time.time()
        request = message_pb2.HeartBeatRequest()
        # Do not drop this line, which is for handling KeyboardInterrupt.
        response = None
        while True:
            try:
                response = self._stub.HeartBeat(request)
            except Exception:
                # grpc disconnect is expect
                response = None
            finally:
                if response is not None:
                    if response.status.code == error_codes_pb2.OK:
                        logger.info("GraphScope coordinator service connected.")
                        break
                time.sleep(1)
                if time.time() - begin_time >= timeout_seconds:
                    if response is None:
                        msg = "grpc connnect failed."
                    else:
                        msg = response.status.error_msg
                    raise ConnectionError("Connect coordinator timeout, {}".format(msg))

    def connect(self, cleanup_instance=True, dangling_timeout_seconds=60):
        return self._connect_session_impl(
            cleanup_instance=cleanup_instance,
            dangling_timeout_seconds=dangling_timeout_seconds,
        )

    @property
    def session_id(self):
        return self._session_id

    def __str__(self):
        return "%s" % self._session_id

    def __repr__(self):
        return str(self)

    def run(self, dag_def):
        return self._run_step_impl(dag_def)

    def fetch_logs(self):
        if self._logs_fetching_thread is None:
            self._logs_fetching_thread = threading.Thread(
                target=self._fetch_logs_impl, args=()
            )
            self._logs_fetching_thread.daemon = True
            self._logs_fetching_thread.start()

    def close(self):
        if self._session_id:
            self._close_session_impl()
            self._session_id = None
        if self._logs_fetching_thread:
            self._logs_fetching_thread.join(timeout=5)

    @catch_grpc_error
    def send_heartbeat(self):
        request = message_pb2.HeartBeatRequest()
        return self._stub.HeartBeat(request)

    @catch_grpc_error
    def _connect_session_impl(self, cleanup_instance=True, dangling_timeout_seconds=60):
        """
        Args:
            cleanup_instance (bool, optional): If True, also delete graphscope
                instance (such as pod) in closing process.
            dangling_timeout_seconds (int, optional): After seconds of client
                disconnect, coordinator will kill this graphscope instance.
                Disable dangling check by setting -1.

        """
        request = message_pb2.ConnectSessionRequest(
            cleanup_instance=cleanup_instance,
            dangling_timeout_seconds=dangling_timeout_seconds,
            version=__version__,
            reconnect=self._reconnect,
        )

        response = self._stub.ConnectSession(request)
        response = check_grpc_response(response)

        self._session_id = response.session_id
        return (
            response.session_id,
            response.cluster_type,
            json.loads(response.engine_config),
            response.pod_name_list,
            response.num_workers,
            response.namespace,
        )

    @suppress_grpc_error
    def _fetch_logs_impl(self):
        request = message_pb2.FetchLogsRequest(session_id=self._session_id)
        responses = self._stub.FetchLogs(request)
        for resp in responses:
            resp = check_grpc_response(resp)
            info = resp.info_message.rstrip()
            if info:
                logger.info(info, extra={"simple": True})
            error = resp.error_message.rstrip()
            if error:
                logger.error(error, extra={"simple": True})

    @catch_grpc_error
    def _close_session_impl(self):
        request = message_pb2.CloseSessionRequest(session_id=self._session_id)
        response = self._stub.CloseSession(request)
        return check_grpc_response(response)

    @catch_grpc_error
    def _run_step_impl(self, dag_def):
        request = message_pb2.RunStepRequest(
            session_id=self._session_id, dag_def=dag_def
        )
        response = self._stub.RunStep(request)
        return check_grpc_response(response)
