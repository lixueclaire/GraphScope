#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited.
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

import base64
import json
import logging
import os
import random
import shutil
import subprocess
import sys
import time
from abc import ABCMeta
from abc import abstractmethod

from graphscope.proto import types_pb2

from gscoordinator.io_utils import PipeWatcher
from gscoordinator.utils import ANALYTICAL_ENGINE_PATH
from gscoordinator.utils import INTERACTIVE_ENGINE_SCRIPT
from gscoordinator.utils import WORKSPACE
from gscoordinator.utils import ResolveMPICmdPrefix
from gscoordinator.utils import get_timestamp
from gscoordinator.utils import is_port_in_use
from gscoordinator.utils import parse_as_glog_level

logger = logging.getLogger("graphscope")


class Launcher(metaclass=ABCMeta):
    def __init__(self):
        self._instance_id = None
        self._num_workers = None
        self._analytical_engine_endpoint = None

    @property
    def analytical_engine_endpoint(self):
        if self._analytical_engine_endpoint is None:
            raise RuntimeError("Get None value of analytical engine endpoint.")
        return str(self._analytical_engine_endpoint)

    @property
    def num_workers(self):
        if self._num_workers is None:
            raise RuntimeError("Get None value of workers number.")
        return int(self._num_workers)

    @property
    def instance_id(self):
        if self._instance_id is None:
            raise RuntimeError("Get None value of instance id.")
        return self._instance_id

    @abstractmethod
    def type(self):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self, is_dangling=False):
        pass

    @abstractmethod
    def poll(self):
        pass


class LocalLauncher(Launcher):
    """
    Launch engine localy with serveral hosts.
    """

    _vineyard_socket_prefix = "/tmp/vineyard.sock."

    def __init__(
        self,
        num_workers,
        hosts,
        vineyard_socket,
        shared_mem,
        log_level,
        instance_id,
        timeout_seconds,
    ):
        super().__init__()
        self._num_workers = num_workers
        self._hosts = hosts
        self._vineyard_socket = vineyard_socket
        self._shared_mem = shared_mem
        self._glog_level = parse_as_glog_level(log_level)
        self._instance_id = instance_id
        self._timeout_seconds = timeout_seconds

        # A graphsope instance may has multiple session by reconnecting to coordinator
        self._instance_workspace = os.path.join(WORKSPACE, self._instance_id)
        os.makedirs(self._instance_workspace, exist_ok=True)
        # setting during client connect to coordinator
        self._session_workspace = None

        # zookeeper
        self._zookeeper_port = None
        self._zetcd_process = None
        # vineyardd
        self._vineyardd_process = None
        # analytical engine
        self._analytical_engine_process = None
        # learning instance processes
        self._learning_instance_processes = {}

        self._closed = True

    def type(self):
        return types_pb2.HOSTS

    def start(self):
        try:
            self._closed = False
            self._create_services()
        except Exception as e:
            logger.error("Error when launching GraphScope locally: %s", str(e))
            self.stop()
            return False
        return True

    def stop(self, is_dangling=False):
        if not self._closed:
            self._stop_interactive_engine_service()
            self._stop_vineyard()
            self._stop_analytical_engine()
            self._closed = True

    def set_session_workspace(self, session_id):
        self._session_workspace = os.path.join(self._instance_workspace, session_id)
        os.makedirs(self._session_workspace, exist_ok=True)

    def distribute_file(self, path):
        dir = os.path.dirname(path)
        for host in self._hosts.split(","):
            if host not in ("localhost", "127.0.0.1"):
                subprocess.check_call(["ssh", host, "mkdir -p {}".format(dir)])
                subprocess.check_call(["scp", path, "{}:{}".format(host, path)])

    def poll(self):
        if self._analytical_engine_process:
            return self._analytical_engine_process.poll()
        return -1

    @property
    def hosts(self):
        return self._hosts

    @property
    def vineyard_socket(self):
        return self._vineyard_socket

    @property
    def zookeeper_port(self):
        return self._zookeeper_port

    def get_engine_config(self):
        config = {
            "engine_hosts": self.hosts,
            "mars_endpoint": None,
        }
        return config

    def get_vineyard_stream_info(self):
        return "ssh", self._hosts.split(",")

    def _get_free_port(self, host):
        port = random.randint(60001, 65535)
        while is_port_in_use(host, port):
            port = random.randint(60001, 65535)
        return port

    def create_interactive_instance(self, config: dict):
        """
        Args:
            config (dict): dict of op_def_pb2.OpDef.attr.
        """
        object_id = config[types_pb2.VINEYARD_ID].i
        schema_path = config[types_pb2.SCHEMA_PATH].s.decode()
        # engine params format:
        #   k1:v1;k2:v2;k3:v3
        engine_params = {}
        if types_pb2.GIE_GREMLIN_ENGINE_PARAMS in config:
            engine_params = json.loads(
                config[types_pb2.GIE_GREMLIN_ENGINE_PARAMS].s.decode()
            )
        engine_params = [
            "{}:{}".format(key, value) for key, value in engine_params.items()
        ]
        enable_gaia = config[types_pb2.GIE_ENABLE_GAIA].b
        cmd = [
            INTERACTIVE_ENGINE_SCRIPT,
            "create_gremlin_instance_on_local",
            self._session_workspace,
            str(object_id),
            schema_path,
            "1",  # server id
            self.vineyard_socket,
            str(self.zookeeper_port),
            "'{}'".format(";".join(engine_params)),
            str(enable_gaia),
        ]
        logger.info("Create GIE instance with command: {0}".format(" ".join(cmd)))
        process = subprocess.Popen(
            cmd,
            start_new_session=True,
            cwd=os.getcwd(),
            env=os.environ.copy(),
            universal_newlines=True,
            encoding="utf-8",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )
        return process

    def close_interactive_instance(self, object_id):
        cmd = [
            INTERACTIVE_ENGINE_SCRIPT,
            "close_gremlin_instance_on_local",
            self._session_workspace,
            str(object_id),
        ]
        logger.info("Close GIE instance with command: {0}".format(" ".join(cmd)))
        process = subprocess.Popen(
            cmd,
            start_new_session=True,
            cwd=os.getcwd(),
            env=os.environ.copy(),
            universal_newlines=True,
            encoding="utf-8",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )
        return process

    def _launch_zetcd(self):
        self._zookeeper_port = self._get_free_port(self._hosts.split(",")[0])

        zetcd_cmd = shutil.which("zetcd")
        if not zetcd_cmd:
            raise RuntimeError("zetcd command not found.")
        cmd = [
            zetcd_cmd,
            "--zkaddr",
            "0.0.0.0:{}".format(self._zookeeper_port),
            "--endpoints",
            "localhost:2379",  # FIXME: get etcd port from vineyard
        ]

        process = subprocess.Popen(
            cmd,
            start_new_session=True,
            cwd=os.getcwd(),
            env=os.environ.copy(),
            universal_newlines=True,
            encoding="utf-8",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )

        logger.info("Server is initializing zetcd.")
        self._zetcd_process = process

        start_time = time.time()
        while not is_port_in_use(self._hosts.split(",")[0], self._zookeeper_port):
            time.sleep(1)
            if (
                self._timeout_seconds
                and self._timeout_seconds + start_time < time.time()
            ):
                raise RuntimeError("Launch zetcd proxy service failed.")
        logger.info(
            "ZEtcd is ready, endpoint is localhost:{0}".format(self._zookeeper_port)
        )

    def _find_vineyardd(self):
        vineyardd = ""
        if "VINEYARD_HOME" in os.environ:
            vineyardd = os.path.expandvars("$VINEYARD_HOME/vineyardd")
        if not vineyardd:
            vineyardd = shutil.which("vineyardd")
        if not vineyardd:
            vineyardd = "vineyardd"
        return vineyardd

    def _create_vineyard(self):
        if not self._vineyard_socket:
            ts = get_timestamp()
            vineyard_socket = "{0}{1}".format(self._vineyard_socket_prefix, ts)
            cmd = [self._find_vineyardd()]
            cmd.extend(["--socket", vineyard_socket])
            cmd.extend(["--size", self._shared_mem])
            cmd.extend(["--etcd_prefix", "vineyard.gsa.{0}".format(ts)])
            env = os.environ.copy()
            env["GLOG_v"] = str(self._glog_level)

            logger.info("Launch vineyardd with command: {0}".format(" ".join(cmd)))

            process = subprocess.Popen(
                cmd,
                start_new_session=True,
                cwd=os.getcwd(),
                env=env,
                universal_newlines=True,
                encoding="utf-8",
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,
            )

            logger.info("Server is initializing vineyardd.")
            stdout_watcher = PipeWatcher(process.stdout, sys.stdout)
            setattr(process, "stdout_watcher", stdout_watcher)

            self._vineyard_socket = vineyard_socket
            self._vineyardd_process = process

    def _create_services(self):
        # create vineyard
        self._create_vineyard()
        # create GAE rpc service
        self._start_analytical_engine()
        # create zetcd
        self._launch_zetcd()

    def _start_analytical_engine(self):
        rmcp = ResolveMPICmdPrefix()
        cmd, mpi_env = rmcp.resolve(self._num_workers, self._hosts)

        master = self._hosts.split(",")[0]
        rpc_port = self._get_free_port(master)
        self._analytical_engine_endpoint = "{}:{}".format(master, str(rpc_port))

        cmd.append(ANALYTICAL_ENGINE_PATH)
        cmd.extend(["--host", "0.0.0.0"])
        cmd.extend(["--port", str(rpc_port)])

        if rmcp.openmpi():
            cmd.extend(["-v", str(self._glog_level)])
        else:
            mpi_env["GLOG_v"] = str(self._glog_level)

        if self._vineyard_socket:
            cmd.extend(["--vineyard_socket", self._vineyard_socket])

        env = os.environ.copy()
        env.update(mpi_env)

        logger.info("Launch analytical engine with command: {}".format(" ".join(cmd)))

        process = subprocess.Popen(
            cmd,
            start_new_session=True,
            cwd=os.getcwd(),
            env=env,
            universal_newlines=True,
            encoding="utf-8",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
        )

        logger.info("Server is initializing analytical engine.")
        stdout_watcher = PipeWatcher(process.stdout, sys.stdout)
        stderr_watcher = PipeWatcher(process.stderr, sys.stderr)
        setattr(process, "stdout_watcher", stdout_watcher)
        setattr(process, "stderr_watcher", stderr_watcher)

        self._analytical_engine_process = process

    def create_learning_instance(self, object_id, handle, config):
        # prepare argument
        handle = json.loads(base64.b64decode(handle.encode("utf-8")).decode("utf-8"))

        server_list = []
        for i in range(self._num_workers):
            server_list.append(
                "localhost:{0}".format(str(self._get_free_port("localhost")))
            )
        hosts = ",".join(server_list)
        handle["server"] = hosts
        handle = base64.b64encode(json.dumps(handle).encode("utf-8")).decode("utf-8")

        # launch the server
        self._learning_instance_processes[object_id] = []
        for index in range(self._num_workers):
            cmd = [
                sys.executable,
                "-m",
                "gscoordinator.learning",
                handle,
                config,
                str(index),
            ]
            logger.info("launching learning server: %s", " ".join(cmd))
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            stdout_watcher = PipeWatcher(proc.stdout, sys.stdout)
            setattr(proc, "stdout_watcher", stdout_watcher)
            self._learning_instance_processes[object_id].append(proc)

        return server_list

    def close_learning_instance(self, object_id):
        if object_id not in self._learning_instance_processes:
            return

        # terminate the process
        for proc in self._learning_instance_processes[object_id]:
            self._stop_subprocess(proc, kill=True)
        self._learning_instance_processes.clear()

    def _stop_vineyard(self):
        self._stop_subprocess(self._vineyardd_process)

    def _stop_interactive_engine_service(self):
        self._stop_subprocess(self._zetcd_process)

    def _stop_analytical_engine(self):
        self._stop_subprocess(self._analytical_engine_process)
        self._analytical_engine_endpoint = None

    def _stop_subprocess(self, proc, kill=False):
        if proc:
            if kill:
                proc.kill()
            else:
                proc.terminate()
            proc.wait()
            proc = None
