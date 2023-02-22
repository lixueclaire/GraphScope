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


from graphscope.framework.app import AppAssets
from graphscope.framework.app import not_compatible_for
from graphscope.framework.app import project_to_simple

__all__ = ["pagerank"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def pagerank(graph, delta=0.85, max_round=10):
    """Evalute PageRank on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        delta (float, optional): Dumping factor. Defaults to 0.85.
        max_round (int, optional): Maximum number of rounds. Defaults to 10.

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex assigned with the pagerank value, evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.flash.pagerank(pg, delta=0.85, max_round=10)
        >>> sess.close()
    """
    delta = float(delta)
    max_round = int(max_round)
    return AppAssets(algo="flash_pagerank", context="vertex_data")(graph, max_round, delta)
