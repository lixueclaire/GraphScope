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

import json

from graphscope.framework.app import AppAssets
from graphscope.framework.app import not_compatible_for
from graphscope.framework.app import project_to_simple

__all__ = ["seal_path"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def seal_path(graph, pairs, k=5, n=10, prefix="."):
    """Evalute VoteRank on a graph.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        pairs: the src-dst pair list to find the path.
        k: path length limit.
        n: the max path num.

    Returns:
        :path : list
        the path list with format pair_ids:path_vertex_id:path_len



    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> c = graphscope.seal_path(g, [[0, 3], [1, 5]], 5, 10)
        >>> sess.close()
    """
    ctx = AppAssets(algo="seal_path", context="tensor")(graph, json.dumps(pairs), k, n, prefix)
