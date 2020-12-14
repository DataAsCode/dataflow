from collections import defaultdict
import networkx as nx

from dbflow.graph.plot_utils import plot_layers


def invert_weight(graph, weight='weight'):
    for start, end in graph.edges:
        graph[start][end][weight] = -1

    return graph


def longest_path(graph, source, target, weight='weight'):
    for start, end in graph.edges:
        if not weight in graph[start][end]:
            graph[start][end][weight] = 1

    graph = invert_weight(graph, weight)
    path = nx.dijkstra_path(graph, source, target)
    invert_weight(graph, weight)

    return len(path) - 2


def plot_graph(graph, root=None):
    node_layers = defaultdict(list)
    for node in graph.nodes:
        if node != root:
            node_layers[longest_path(graph, source=root, target=node)].append(node)

    layers = []
    for layer, nodes in sorted(node_layers.items()):
        if layer == 0:
            layers.append({node: {} for node in nodes})
        else:
            edges = {}
            for node in nodes:
                node_edges = {node: 0 for node in layers[-1].keys()}

                for predecesor in graph.predecessors(node):
                    node_edges[predecesor] = 1

                    for layer in layers[::-1]:
                        if not predecesor in layer and layer != layers[0]:
                            layer[predecesor] = {predecesor: 0.3}

                edges[node] = node_edges

            layers.append(edges)

    return plot_layers(layers)
