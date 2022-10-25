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

class BellmanFord:

    def __init__(self, g=None):
        """
        Constructor for BellmanFord object

        :param: g: the graph inititially provided by the client
        """
        self.vertices = set()    #the number of vertices stored as a set   
        self.edges = {}   #edge storage
        if g is not None: 
            self.buildEdge(g)
    
    def buildEdge(self, g):
        """
        Helper function to parse the provided graph for vertices and edges, then add
        them to our internal states.

        :param: g: the graph inititially provided by the client
        """

        #parse the top dictionary
        for i in g:
            #parse the sub dictionary
            for j in g[i]:
                edge = [i, j, g[i][j]]
                #add the vertices
                self.add_vertices(i, j)

                #then add the edge
                self.add_edge(edge)

    def add_vertices(self, ver1, ver2):
        #set will make sure there's no replicas of the same vertice
        self.vertices.add(ver1)
        self.vertices.add(ver2)


    def add_edge(self,edge):
        """
        Function to add edges to our edge storage

        :param: edge: a tuple in format (currency1, currency2, weight)
        """
        curr1, curr2, weight = edge[0:]
        #update the vertices set
        self.add_vertices(curr1, curr2)

        #add to the edge storage
        if curr1 not in self.edges:
            self.edges[curr1] = {}
        
        self.edges[curr1][curr2] = weight

    def remove_edge(self, curr1, curr2):
        """
        Helper function to remove the specified edge
        """

        #only try to remove when keys are valid
        if curr1 in self.edges and curr2 in self.edges[curr1]:
            del self.edges[curr1][curr2]
        else:
            print('Not a valid key. No data was removed.')


    def shortest_paths(self, start_vertex, tolerance):
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
        #Initialize all distances to be infinity, except start_vertex = 0
        dist = {}
        pred = {}
        neg_cycle = tuple()

        for v in self.vertices:
            dist[v] = float("inf")
            pred[v] = None

        dist[start_vertex] = 0

        #Run Bellman Ford algorithm to find all shortest path
        for i in range(len(self.vertices)):
            for u in self.edges:
                for v in self.edges[u]:
                    w = self.edges[u][v]

                    if round(dist[v], 12) - round((dist[u] + w), 12) > tolerance:
                        if v == start_vertex:
                            #print('Found arbitrage  133')
                            #print('----------1. Arbitrage: {}, dist[v]: {}, dist[u]: {}'.format((u,v), dist[v], dist[u] + w))
                            return dist, pred, (u, v)
                    
                    #Update distance otherwise
                        dist[v] = dist[u] + w
                        pred[v] = u

        #Last check after all shortest paths have been identified
        for u in self.edges:
            for v in self.edges[u]:
                w = self.edges[u][v]
                if round((dist[u] + w), 12) < round(dist[v], 12):
                    neg_cycle = (u, v)
                    print('2. found negative cycle: ', neg_cycle)
                    return dist, pred, neg_cycle    #return the first negative cycle found

        return dist, pred, neg_cycle 