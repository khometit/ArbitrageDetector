"""
Use a graph to detect an arbitrage. 
> Node are each currency
> Edges represent a trading from 1 currency to another
> Then use this graph to find pathways that would lead to an arbitrage profit. 
    > Negative-weight cycle = arbitrage
    > Use Bellman-Ford shortest-path algorithm
    > Make the edges negative Log of the current xchange rate, then look for negative cycles in the path using B-F

This module will return the negative cycle path to get reported on.
"""

def shortest_paths(self, start_vertex, tolerance=0):
    """
    Find the shortest paths (sum of edge weights) from start_vertex to every other vertex.
    Also detect if there are negative cycles and report one of them.
    Edges may be negative.

    For relaxation and cycle detection, we use tolerance. Only relaxations resulting in an improvement
    greater than tolerance are considered. For negative cycle detection, if the sum of weights is
    greater than -tolerance it is not reported as a negative cycle. This is useful when circuits are expected
    to be close to zero.

    >>> g = BellmanFord({'a': {'b': 1, 'c':5}, 'b': {'c': 2, 'a': 10}, 'c': {'a': 14, 'd': -3}, 'e': {'a': 100}})
    >>> dist, prev, neg_edge = g.shortest_paths('a')
    >>> [(v, dist[v]) for v in sorted(dist)]  # shortest distance from 'a' to each other vertex
    [('a', 0), ('b', 1), ('c', 3), ('d', 0), ('e', inf)]
    >>> [(v, prev[v]) for v in sorted(prev)]  # last edge in shortest paths
    [('a', None), ('b', 'a'), ('c', 'b'), ('d', 'c'), ('e', None)]
    >>> neg_edge is None
    True
    >>> g.add_edge('a', 'e', -200)
    >>> dist, prev, neg_edge = g.shortest_paths('a')
    >>> neg_edge  # edge where we noticed a negative cycle
    ('e', 'a')

    :param start_vertex: start of all paths
    :param tolerance: only if a path is more than tolerance better will it be relaxed
    :return: distance, predecessor, negative_cycle
        distance:       dictionary keyed by vertex of shortest distance from start_vertex to that vertex
        predecessor:    dictionary keyed by vertex of previous vertex in shortest path from start_vertex
        negative_cycle: None if no negative cycle, otherwise an edge, (u,v), in one such cycle
    """