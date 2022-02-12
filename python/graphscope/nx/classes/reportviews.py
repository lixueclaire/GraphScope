from collections.abc import Mapping
from collections.abc import Set

from graphscope.nx import NetworkXError


# NodeViews
class NodeView(Mapping, Set):
    """A NodeView class to act as G.nodes for a NetworkX Graph

    Set operations act on the nodes without considering data.
    Iteration is over nodes. Node data can be looked up like a dict.
    Use NodeDataView to iterate over node data or to specify a data
    attribute for lookup. NodeDataView is created by calling the NodeView.

    Parameters
    ----------
    graph : NetworkX graph-like class

    Examples
    --------
    >>> G = nx.path_graph(3)
    >>> NV = G.nodes()
    >>> 2 in NV
    True
    >>> for n in NV:
    ...     print(n)
    0
    1
    2
    >>> assert NV & {1, 2, 3} == {1, 2}

    >>> G.add_node(2, color="blue")
    >>> NV[2]
    {'color': 'blue'}
    >>> G.add_node(8, color="red")
    >>> NDV = G.nodes(data=True)
    >>> (2, NV[2]) in NDV
    True
    >>> for n, dd in NDV:
    ...     print((n, dd.get("color", "aqua")))
    (0, 'aqua')
    (1, 'aqua')
    (2, 'blue')
    (8, 'red')
    >>> NDV[2] == NV[2]
    True

    >>> NVdata = G.nodes(data="color", default="aqua")
    >>> (2, NVdata[2]) in NVdata
    True
    >>> for n, dd in NVdata:
    ...     print((n, dd))
    (0, 'aqua')
    (1, 'aqua')
    (2, 'blue')
    (8, 'red')
    >>> NVdata[2] == NV[2]  # NVdata gets 'color', NV gets datadict
    False
    """

    __slots__ = ("_graph", "_nodes",)

    def __getstate__(self):
        return {"_graph": self._graph, "_nodes": self._nodes}

    def __setstate__(self, state):
        self._graph = state["_graph"]
        self._nodes = state["_nodes"]

    def __init__(self, graph):
        self._graph = graph
        self._nodes = graph._node

    # Mapping methods
    def __len__(self):
        return len(self._nodes)

    def __iter__(self):
        return iter(self._nodes)

    def __getitem__(self, n):
        if isinstance(n, slice):
            raise NetworkXError(
                f"{type(self).__name__} does not support slicing, "
                f"try list(G.nodes)[{n.start}:{n.stop}:{n.step}]"
            )
        return self._nodes[n]

    # Set methods
    def __contains__(self, n):
        return n in self._graph

    @classmethod
    def _from_iterable(cls, it):
        return set(it)

    # DataView method
    def __call__(self, data=False, default=None):
        if data is False:
            return self
        return NodeDataView(self._nodes, data, default)

    def data(self, data=True, default=None):
        if data is False:
            return self
        return NodeDataView(self._nodes, data, default)

    def __str__(self):
        return str(list(self))

    def __repr__(self):
        return f"{self.__class__.__name__}({tuple(self)})"


class NodeDataView(Set):
    """A DataView class for nodes of a NetworkX Graph

    The main use for this class is to iterate through node-data pairs.
    The data can be the entire data-dictionary for each node, or it
    can be a specific attribute (with default) for each node.
    Set operations are enabled with NodeDataView, but don't work in
    cases where the data is not hashable. Use with caution.
    Typically, set operations on nodes use NodeView, not NodeDataView.
    That is, they use `G.nodes` instead of `G.nodes(data='foo')`.

    Parameters
    ==========
    graph : NetworkX graph-like class
    data : bool or string (default=False)
    default : object (default=None)
    """

    __slots__ = ("_nodes", "_data", "_default")

    def __getstate__(self):
        return {"_nodes": self._nodes, "_data": self._data, "_default": self._default}

    def __setstate__(self, state):
        self._nodes = state["_nodes"]
        self._data = state["_data"]
        self._default = state["_default"]

    def __init__(self, nodedict, data=False, default=None):
        self._nodes = nodedict
        self._data = data
        self._default = default

    @classmethod
    def _from_iterable(cls, it):
        try:
            return set(it)
        except TypeError as err:
            if "unhashable" in str(err):
                msg = " : Could be b/c data=True or your values are unhashable"
                raise TypeError(str(err) + msg) from err
            raise

    def __len__(self):
        return len(self._nodes)

    def __iter__(self):
        data = self._data
        if data is False:
            return iter(self._nodes)
        if data is True:
            return iter(self._nodes.items())
        return (
            (n, dd[data] if data in dd else self._default)
            for n, dd in self._nodes.items()
        )

    def __contains__(self, n):
        try:
            node_in = n in self._nodes
        except TypeError:
            n, d = n
            return n in self._nodes and self[n] == d
        if node_in is True:
            return node_in
        try:
            n, d = n
        except (TypeError, ValueError):
            return False
        return n in self._nodes and self[n] == d

    def __getitem__(self, n):
        if isinstance(n, slice):
            raise NetworkXError(
                f"{type(self).__name__} does not support slicing, "
                f"try list(G.nodes.data())[{n.start}:{n.stop}:{n.step}]"
            )
        ddict = self._nodes[n]
        data = self._data
        if data is False or data is True:
            return ddict
        return ddict[data] if data in ddict else self._default

    def __str__(self):
        return str(list(self))

    def __repr__(self):
        name = self.__class__.__name__
        if self._data is False:
            return f"{name}({tuple(self)})"
        if self._data is True:
            return f"{name}({dict(self)})"
        return f"{name}({dict(self)}, data={self._data!r})"


# EdgeDataViews
class OutEdgeDataView:
    """EdgeDataView for outward edges of DiGraph; See EdgeDataView"""

    __slots__ = (
        "_viewer",
        "_nbunch",
        "_data",
        "_default",
        "_adjdict",
        "_nodes_nbrs",
        "_report",
    )

    def __getstate__(self):
        return {
            "viewer": self._viewer,
            "nbunch": self._nbunch,
            "data": self._data,
            "default": self._default,
        }

    def __setstate__(self, state):
        self.__init__(**state)

    def __init__(self, viewer, nbunch=None, data=False, default=None):
        self._viewer = viewer
        adjdict = self._adjdict = viewer._adjdict
        if nbunch is None:
            self._nodes_nbrs = adjdict.items
        else:
            # dict retains order of nodes but acts like a set
            nbunch = dict.fromkeys(viewer._graph.nbunch_iter(nbunch))
            self._nodes_nbrs = lambda: [(n, adjdict[n]) for n in nbunch]
        self._nbunch = nbunch
        self._data = data
        self._default = default
        # Set _report based on data and default
        if data is True:
            self._report = lambda n, nbr, dd: (n, nbr, dd)
        elif data is False:
            self._report = lambda n, nbr, dd: (n, nbr)
        else:  # data is attribute name
            self._report = (
                lambda n, nbr, dd: (n, nbr, dd[data])
                if data in dd
                else (n, nbr, default)
            )

    def __len__(self):
        return self._viewer._graph.number_of_edges()

    def __iter__(self):
        return (
            self._report(n, nbr, dd)
            for n, nbrs in self._nodes_nbrs()
            for nbr, dd in nbrs.items()
        )

    def __contains__(self, e):
        u, v = e[:2]
        if self._nbunch is not None and u not in self._nbunch:
            return False  # this edge doesn't start in nbunch
        try:
            ddict = self._adjdict[u][v]
        except KeyError:
            return False
        return e == self._report(u, v, ddict)

    def __str__(self):
        return str(list(self))

    def __repr__(self):
        return f"{self.__class__.__name__}({list(self)})"


class EdgeDataView(OutEdgeDataView):
    """A EdgeDataView class for edges of Graph

    This view is primarily used to iterate over the edges reporting
    edges as node-tuples with edge data optionally reported. The
    argument `nbunch` allows restriction to edges incident to nodes
    in that container/singleton. The default (nbunch=None)
    reports all edges. The arguments `data` and `default` control
    what edge data is reported. The default `data is False` reports
    only node-tuples for each edge. If `data is True` the entire edge
    data dict is returned. Otherwise `data` is assumed to hold the name
    of the edge attribute to report with default `default` if  that
    edge attribute is not present.

    Parameters
    ----------
    nbunch : container of nodes, node or None (default None)
    data : False, True or string (default False)
    default : default value (default None)

    Examples
    --------
    >>> G = nx.path_graph(3)
    >>> G.add_edge(1, 2, foo="bar")
    >>> list(G.edges(data="foo", default="biz"))
    [(0, 1, 'biz'), (1, 2, 'bar')]
    >>> assert (0, 1, "biz") in G.edges(data="foo", default="biz")
    """

    __slots__ = ()

    def __len__(self):
        return sum(1 for e in self)

    def __iter__(self):
        seen = {}
        for n, nbrs in self._nodes_nbrs():
            for nbr, dd in nbrs.items():
                if nbr not in seen:
                    yield self._report(n, nbr, dd)
            seen[n] = 1
        del seen

    def __contains__(self, e):
        u, v = e[:2]
        if self._nbunch is not None and u not in self._nbunch and v not in self._nbunch:
            return False  # this edge doesn't start and it doesn't end in nbunch
        try:
            ddict = self._adjdict[u][v]
        except KeyError:
            return False
        return e == self._report(u, v, ddict)


class InEdgeDataView(OutEdgeDataView):
    """An EdgeDataView class for outward edges of DiGraph; See EdgeDataView"""

    __slots__ = ()

    def __iter__(self):
        return (
            self._report(nbr, n, dd)
            for n, nbrs in self._nodes_nbrs()
            for nbr, dd in nbrs.items()
        )

    def __contains__(self, e):
        u, v = e[:2]
        if self._nbunch is not None and v not in self._nbunch:
            return False  # this edge doesn't end in nbunch
        try:
            ddict = self._adjdict[v][u]
        except KeyError:
            return False
        return e == self._report(u, v, ddict)


# EdgeViews    have set operations and no data reported
class OutEdgeView(Set, Mapping):
    """A EdgeView class for outward edges of a DiGraph"""

    __slots__ = ("_adjdict", "_graph", "_nodes_nbrs")

    def __getstate__(self):
        return {"_graph": self._graph}

    def __setstate__(self, state):

        self._graph = G = state["_graph"]
        self._adjdict = G._succ if hasattr(G, "succ") else G._adj
        self._nodes_nbrs = self._adjdict.items

    @classmethod
    def _from_iterable(cls, it):
        return set(it)

    dataview = OutEdgeDataView

    def __init__(self, G):
        self._graph = G
        self._adjdict = G._succ if hasattr(G, "succ") else G._adj
        self._nodes_nbrs = self._adjdict.items

    # Set methods
    def __len__(self):
        return self._graph.number_of_edges()

    def __iter__(self):
        for n, nbrs in self._nodes_nbrs():
            for nbr in nbrs:
                yield (n, nbr)

    def __contains__(self, e):
        try:
            u, v = e
            return v in self._adjdict[u]
        except KeyError:
            return False

    # Mapping Methods
    def __getitem__(self, e):
        if isinstance(e, slice):
            raise NetworkXError(
                f"{type(self).__name__} does not support slicing, "
                f"try list(G.edges)[{e.start}:{e.stop}:{e.step}]"
            )
        u, v = e
        return self._adjdict[u][v]

    # EdgeDataView methods
    def __call__(self, nbunch=None, data=False, default=None):
        if nbunch is None and data is False:
            return self
        return self.dataview(self, nbunch, data, default)

    def data(self, data=True, default=None, nbunch=None):
        if nbunch is None and data is False:
            return self
        return self.dataview(self, nbunch, data, default)

    # String Methods
    def __str__(self):
        return str(list(self))

    def __repr__(self):
        return f"{self.__class__.__name__}({list(self)})"


class EdgeView(OutEdgeView):
    """A EdgeView class for edges of a Graph

    This densely packed View allows iteration over edges, data lookup
    like a dict and set operations on edges represented by node-tuples.
    In addition, edge data can be controlled by calling this object
    possibly creating an EdgeDataView. Typically edges are iterated over
    and reported as `(u, v)` node tuples or `(u, v, key)` node/key tuples
    for multigraphs. Those edge representations can also be using to
    lookup the data dict for any edge. Set operations also are available
    where those tuples are the elements of the set.
    Calling this object with optional arguments `data`, `default` and `keys`
    controls the form of the tuple (see EdgeDataView). Optional argument
    `nbunch` allows restriction to edges only involving certain nodes.

    If `data is False` (the default) then iterate over 2-tuples `(u, v)`.
    If `data is True` iterate over 3-tuples `(u, v, datadict)`.
    Otherwise iterate over `(u, v, datadict.get(data, default))`.
    For Multigraphs, if `keys is True`, replace `u, v` with `u, v, key` above.

    Parameters
    ==========
    graph : NetworkX graph-like class
    nbunch : (default= all nodes in graph) only report edges with these nodes
    keys : (only for MultiGraph. default=False) report edge key in tuple
    data : bool or string (default=False) see above
    default : object (default=None)

    Examples
    ========
    >>> G = nx.path_graph(4)
    >>> EV = G.edges()
    >>> (2, 3) in EV
    True
    >>> for u, v in EV:
    ...     print((u, v))
    (0, 1)
    (1, 2)
    (2, 3)
    >>> assert EV & {(1, 2), (3, 4)} == {(1, 2)}

    >>> EVdata = G.edges(data="color", default="aqua")
    >>> G.add_edge(2, 3, color="blue")
    >>> assert (2, 3, "blue") in EVdata
    >>> for u, v, c in EVdata:
    ...     print(f"({u}, {v}) has color: {c}")
    (0, 1) has color: aqua
    (1, 2) has color: aqua
    (2, 3) has color: blue

    >>> EVnbunch = G.edges(nbunch=2)
    >>> assert (2, 3) in EVnbunch
    >>> assert (0, 1) not in EVnbunch
    >>> for u, v in EVnbunch:
    ...     assert u == 2 or v == 2

    >>> MG = nx.path_graph(4, create_using=nx.MultiGraph)
    >>> EVmulti = MG.edges(keys=True)
    >>> (2, 3, 0) in EVmulti
    True
    >>> (2, 3) in EVmulti  # 2-tuples work even when keys is True
    True
    >>> key = MG.add_edge(2, 3)
    >>> for u, v, k in EVmulti:
    ...     print((u, v, k))
    (0, 1, 0)
    (1, 2, 0)
    (2, 3, 0)
    (2, 3, 1)
    """

    __slots__ = ()

    dataview = EdgeDataView

    def __len__(self):
        return self._graph.number_of_edges()

    def __iter__(self):
        seen = {}
        for n, nbrs in self._nodes_nbrs():
            for nbr in list(nbrs):
                if nbr not in seen:
                    yield (n, nbr)
            seen[n] = 1
        del seen

    def __contains__(self, e):
        try:
            u, v = e[:2]
            return self._graph.has_edge(u, v)
        except ValueError:
            return False


class InEdgeView(OutEdgeView):
    """A EdgeView class for inward edges of a DiGraph"""

    __slots__ = ()

    def __setstate__(self, state):
        self._graph = G = state["_graph"]
        self._adjdict = G._pred if hasattr(G, "pred") else G._adj
        self._nodes_nbrs = self._adjdict.items

    dataview = InEdgeDataView

    def __init__(self, G):
        self._graph = G
        self._adjdict = G._pred if hasattr(G, "pred") else G._adj
        self._nodes_nbrs = self._adjdict.items

    def __iter__(self):
        for n, nbrs in self._nodes_nbrs():
            for nbr in nbrs:
                yield (nbr, n)

    def __contains__(self, e):
        return self._graph.has_edge(*e)

    def __getitem__(self, e):
        if isinstance(e, slice):
            raise NetworkXError(
                f"{type(self).__name__} does not support slicing, "
                f"try list(G.in_edges)[{e.start}:{e.stop}:{e.step}]"
            )
        u, v = e
        return self._adjdict[v][u]
