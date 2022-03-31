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

import io
from timeit import default_timer as timer

import concurrent.futures
from collections import deque
from collections.abc import MutableMapping
from collections import UserDict

import orjson as json
import simdjson

from graphscope.proto import types_pb2
from graphscope.framework import dag_utils

__all__ = ["NodeDict", "AdjDict", "NodeCache", "AdjListCache"]

class Cache:
    def __init__(self, graph):
        self.parser = simdjson.Parser()
        self.nbr_parser = simdjson.Parser()
        self._graph = graph
        self._node_id_cache = None
        self._node_attr_cache = None
        self._neighbor_cache = None
        self._neighbor_attr_cache = None
        self.gid = 0
        self.pre_gid = 0
        self.node_attr_align = False
        self.neighbor_align = False
        self.neighbor_attr_align = False
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self.future = deque()
        self.nbr_future = deque()

    def prewarming(self):
        self.gid = 0
        self.n = 0
        self._fetch_node_id_cache(0)
        self._fetch_neighbor_cache(0)
        # self._fetch_node_attr_cache(0)

    @property
    def node_id_cache(self):
        return self._node_id_cache

    @property
    def node_attr_cache(self):
        return self._node_attr_cache

    @property
    def neighbor_cache(self):
        return self._neighbor_cache

    @property
    def neighbor_attr_cache(self):
        return self._neighbor_attr_cache

    def align_node_attr_cache(self):
        if self.node_attr_align is False:
            self._node_attr_cache = self._get_node_attr_cache(self.pre_gid)
            self.node_attr_align = True

    def align_neighbor_cache(self):
        if self.neighbor_align is False:
            t = timer()
            f = self.nbr_future.pop()
            self._neighbor_cache = f.result()
            print("Pop neighbor cache", timer() - t)
            # self._neighbor_cache = self.nbr_parser.loads(archive.get_bytes())
            # print("Loads neighbor cache", timer() - t)
            if self.n < self._len:
                self._fetch_neighbor_cache(self.pre_gid)
            self.neighbor_align = True

    def align_neighbor_attr_cache(self):
        if self.neighbor_attr_align is False:
            print("call align neighbor attr cache")
            self._neighbor_attr_cache = self._get_neighbor_attr_cache(self.pre_gid)
            self.neighbor_attr_align = True

    def __len__(self):
        return self._graph.number_of_nodes()

    def __iter__(self):
        print("call Cache __iter__")
        self.gid = 0
        self.n = 0
        self._len = self._graph.number_of_nodes()
        while True:
            if self.n >= self._len:
                break
            t = timer()
            f = self.future.pop()
            print("pop result", timer() - t)
            archive = f.result()
            self.gid = archive.get_uint64()
            print("self.gid", self.gid)
            node_size = archive.get_uint32()
            self.n += node_size
            print("self.n", self.n)
            if self.n < self._len:
                self._fetch_node_id_cache(self.gid)
            self.node_attr_align = self.neighbor_align = self.neighbor_attr_align = False
            self.pre_gid = self.gid
            t = timer()
            array = self.parser.parse(archive.get_bytes())
            print("parse node cache", timer() - t)
            t = timer()
            self._node_id_cache = {k: v for v, k in enumerate(array)}
            print("to hash map", timer() - t)
            del array
            for n in self._node_id_cache:
                yield n

    def _fetch_node_id_cache(self, gid):
        print("_fetch_node_id_cache", gid)
        f = self.executor.submit(self._get_node_id_cache, gid)
        self.future.append(f)

    def _fetch_node_attr_cache(self, gid):
        f = self.executor.submit(self._get_node_attr_cache, gid)
        self.future.append(f)

    def _fetch_neighbor_cache(self, gid):
        print("_fetch_neighbor_cache", gid)
        f = self.executor.submit(self._get_neighbor_cache, gid)
        self.nbr_future.append(f)

    def _fetch_neighbor_attr_cache(self, gid):
        f = self.executor.submit(self._get_neighbor_attr_cache, gid)
        self.future.append(f)

    def _get_node_id_cache(self, gid):
        t = timer()
        op = dag_utils.report_graph(self._graph, types_pb2.NODE_ID_CACHE_BY_GID, gid=gid)
        archive = op.eval()
        print("fetch cache", timer() - t)
        return archive

    def _get_node_attr_cache(self, gid):
        print("call _get_data_cache", gid)
        op = dag_utils.report_graph(self._graph, types_pb2.NODE_ATTR_CACHE_BY_GID, gid=gid)
        archive = op.eval()
        node_attr_cache = self.parser.parse(archive.get_bytes())
        return node_attr_cache

    def _get_node_attr(self, key):
        print("call _get_data", key)
        op = dag_utils.report_graph(self._graph, types_pb2.NODE_DATA, node=json.dumps(key))
        archive = op.eval()
        return json.loads(archive.get_string())

    def _get_neighbor_cache(self, gid):
        print("call __get_nbr_cache", gid)
        t = timer()
        op = dag_utils.report_graph(self._graph, types_pb2.NEIGHBOR_BY_GID, gid=gid)
        archive = op.eval()
        print("fetch neighbor cache", timer() -t)
        t = timer()
        file = io.BytesIO(archive.get_bytes())
        neighbor_cache = simdjson.load(file)
        print("Loads neighbor cache", timer() - t)
        return neighbor_cache

    def _get_neighbor_attr_cache(self, gid):
        print("call __get_nbr_attr_cache", gid)
        op = dag_utils.report_graph(self._graph, types_pb2.NEIGHBOR_ATTR_BY_GID, gid=gid)
        archive = op.eval()
        neighbor_attr_cache = self.parser.parse(archive.get_bytes())
        return neighbor_attr_cache


class NodeDictPoc:
    def __init__(self, cache):
        self._graph = cache._graph
        self._cache = cache

    def __repr__(self):
        return f"NodeDictPoc"

    def __len__(self):
        return len(self._cache)

    def __getitem__(self, key):
        if key in self._cache.node_id_cache:
            self._cache.align_node_attr_cache()
            map_index = self._cache.node_id_cache[key]
            return NodeAttrDictPoc(self._graph, key, self._cache.node_attr_cache[map_index])
        elif key in self._graph:
            return self._cache.__missing__(key)
        else:
            raise KeyError(key)

    def __setitem__(self, key, item):
        if key in self._cache.node_id_cache and self._cache.node_attr_align:
            index = self._cache.node_id_cache[key]
            self._cache.node_attr_cache[index] = item
            self._graph.set_node_data(key, item)
        elif key in self._graph:
            self._graph.set_node_data(key, item)
        else:
            self._graph.add_node(key, **item)

    def __delitem__(self, key):
        if key in self._cache.node_id_cache:
            self._cache.node_id_cache.pop(key)
        self._graph.remove_node(key)

    def __iter__(self):
        return iter(self._cache)

    def __contains__(self, key):
        return key in self._cache


class NodeAttrDictPoc(UserDict):
    def __init__(self, graph, key, data):
        super().__init__()
        self.graph = graph
        self.key = key
        self.data = data

    def __setitem__(self, key, item):
        super().__setitem__(key, item)
        self.graph.set_node_data(self.key, self.data)

    def __delitem__(self, key):
        super().__delitem__(key)
        self.graph.set_node_data(self.key, self.data)

    def clear(self):
        super().clear()
        self.graph.set_node_data(self.key, self.data)

    def update(self, dict=None, **kwargs):
        super().update(dict, **kwargs)
        self.graph.set_node_data(self.key, self.data)


class AdjListDictPoc:
    def __init__(self, cache):
        self._graph = cache._graph
        self._cache = cache

    def __repr__(self):
        return f"AdjListDictPoc"

    def __len__(self):
        return len(self._cache)

    def __getitem__(self, key):
        if key in self._cache.node_id_cache:
            self._cache.align_neighbor_cache()
            map_index = self._cache.node_id_cache[key]
            return NeighborDict(self._graph, self._cache, key, self._cache.neighbor_cache[map_index])
        elif key in self._graph:
            return self.__missing__(key)
        else:
            raise KeyError(key)

    def __setitem__(self, key, item):
        raise NotImplementedError("hard to implement this method in rpc way")

    def __delitem__(self, key):
        # NB: not really identical to del g._adj[key]
        if key in self._cache.node_id_cache:
            self._cache.node_id_cache.pop(key)
        self._graph.remove_node(key)

    def __iter__(self):
        print("call AdjListDictPoc _iter_")
        return iter(self._cache)

    def __contains__(self, key):
        return key in self._cache


class NeighborDict(UserDict):
    def __init__(self, graph, cache, key, data):
        super().__init__()
        self._graph = graph
        self._cache = cache
        self._key = key
        self.nbr_list = data
        # self.nbr2i = {k: v for v, k in enumerate(data)}
        # self.attred = False

    def __len__(self):
        return len(self.nbr_list)

    def __getitem__(self, key):
        if key in self.nbr2i:
            if self.attred is False:
                self._cache.align_neighbor_attr_cache()
                map_index = self._cache.node_id_cache[self._key]
                self.nbr_attr = self._cache.neighbor_attr_cache[map_index]
                self.attred = True
            return NeighborAttrDict(self._graph, self._key, key, self.nbr_attr[self.nbr2i[key]])
        raise KeyError(key)

    def __iter__(self):
        return iter(self.nbr_list)


class NeighborAttrDict(UserDict):
    def __init__(self, graph, u, v, data):
        super().__init__()
        self.graph = graph
        self.u = u
        self.v = v
        self.data = data

    def __setitem__(self, key, item):
        super().__setitem__(key, item)
        print("set edge data", self.u, self.v, self.data)
        self.graph.set_edge_data(self.u, self.v, self.data)

    def __delitem__(self, key):
        super().__delitem__(key)
        self.graph.set_edge_data(self.u, self.v, self.data)

    def clear(self):
        super().clear()
        self.graph.set_edge_data(self.u, self.v, self.data)

    def update(self, dict=None, **kwargs):
        super().update(dict, **kwargs)
        self.graph.set_edge_data(self.u, self.v, self.data)


class NodeDict(MutableMapping):
    __slots__ = "_graph"

    def __init__(self, graph):
        self._graph = graph

    def __len__(self):
        return self._graph.number_of_nodes()

    def __getitem__(self, key):
        if key not in self._graph:
            raise KeyError(key)
        return NodeAttrDict(self._graph, key)

    def __setitem__(self, key, value):
        if key not in self._graph:
            self._graph.add_nodes_from([key, value])
        else:
            self._graph.set_node_data(key, value)

    def __delitem__(self, key):
        self._graph.remove_node(key)

    def __iter__(self):
        # batch get nodes
        node_num = self.__len__()
        count = 0
        pos = (0, 0, 0)  # start iterate from the (worker:0, lid:0, label_id:0) node.
        while count < node_num:
            while True:
                ret = self._graph._batch_get_node(pos)
                pos = ret["next"]
                if ret["status"] is True:
                    break
            batch = ret["batch"]
            count += len(batch)
            for node in batch:
                yield tuple(node) if isinstance(node, list) else node


class NodeAttrDict(MutableMapping):
    __slots__ = ("_graph", "_node", "mapping")

    def __init__(self, graph, node, data=None):
        self._graph = graph
        self._node = node
        if data:
            self.mapping = data
        else:
            self.mapping = graph.get_node_data(node)

    def __len__(self):
        return len(self.mapping)

    def __getitem__(self, key):
        return self.mapping[key]

    def __setitem__(self, key, value):
        self.mapping[key] = value
        self._graph.set_node_data(self._node, self.mapping)

    def __delitem__(self, key):
        del self.mapping[key]
        self._graph.set_node_data(self._node, self.mapping)

    def __iter__(self):
        return iter(self.mapping)

    def copy(self):
        return self.mapping

    def __repr__(self):
        return str(self.mapping)


# NB: implement the dict structure to reuse the views of networkx. since we
# fetch the neighbor messages of node one by one, it's slower than reimplemented
# views.
# NB: maybe we can reimpl keysview, valuesview and itemsview for the AdjDict.
class AdjDict(MutableMapping):
    __slots__ = ("_graph", "_rtype")

    def __init__(self, graph, rtype=types_pb2.SUCCS_BY_NODE):
        self._graph = graph
        self._rtype = rtype

    def __len__(self):
        return self._graph.number_of_nodes()

    def __getitem__(self, key):
        hash(key)  # check the key is hashable.
        if key not in self._graph:
            raise KeyError(key)
        return AdjInnerDict(self._graph, key, self._rtype)

    def __setitem__(self, key, value):
        raise NotImplementedError("hard to implement this method in rpc way")

    def __delitem__(self, key):
        # NB: not really identical to del g._adj[key]
        self._graph.remove_node(key)

    def __iter__(self):
        # batch get nodes
        node_num = self.__len__()
        count = 0
        pos = (0, 0, 0)  # start iterate from the (worker:0, lid:0, label_id:0) node.
        while count < node_num:
            while True:
                ret = self._graph._batch_get_node(pos)
                pos = ret["next"]
                if ret["status"] is True:
                    break
            batch = ret["batch"]
            count += len(batch)
            for node in batch:
                yield tuple(node) if isinstance(node, list) else node

    def __repr__(self):
        return f"{type(self).__name__}"


class AdjInnerDict(MutableMapping):
    __slots__ = ("_graph", "_node", "_type", "mapping")

    def __init__(self, graph, node, rtype):
        self._graph = graph
        self._node = node
        self._type = rtype
        self.mapping = graph._get_nbrs(node, rtype)

    def __len__(self):
        return len(self.mapping)

    def __getitem__(self, key):
        if key not in self.mapping:
            raise KeyError(key)
        return AdjEdgeAttrDict(self._graph, self._node, key, self.mapping[key])

    def __setitem__(self, key, value):
        if key in self.mapping:
            self._graph.set_edge_data(self._node, key, value)
        else:
            self._graph.add_edges_from([(self._node, key, value)])
        self.mapping[key] = value

    def __delitem__(self, key):
        if key in self.mapping:
            del self.mapping[key]
            self._graph.remove_edge(self._node, key)

    def __iter__(self):
        return iter(self.mapping)

    def __str__(self):
        return str(self.mapping)

    def __repr__(self):
        return f"{type(self).__name__}({self.mapping})"

    def copy(self):
        return self.mapping


class AdjEdgeAttrDict(MutableMapping):
    __slots__ = ("_graph", "_node", "_nbr", "mapping")

    def __init__(self, graph, node, nbr, data):
        self._graph = graph
        self._node = node
        self._nbr = nbr
        self.mapping = data

    def __len__(self):
        return len(self.mapping)

    def __getitem__(self, key):
        return self.mapping[key]

    def __setitem__(self, key, value):
        self.mapping[key] = value
        self._graph.set_edge_data(self._node, self._nbr, self.mapping)

    def __delitem__(self, key):
        if key in self.mapping:
            del self.mapping[key]
            self._graph.set_edge_data(self._node, self._nbr, self.mapping)

    def __iter__(self):
        return iter(self.mapping)

    def copy(self):
        return self.mapping

    def __str__(self):
        return str(self.mapping)

    def __repr__(self):
        return f"{type(self).__name__}({self.mapping})"
