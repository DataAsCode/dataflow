import itertools
import numpy as np
import matplotlib.pyplot as plt
import netgraph


def plot_layered_network(weight_matrices,
                         distance_between_layers=2,
                         distance_between_nodes=1,
                         layer_labels=None,
                         **kwargs):
    """
    Convenience function to plot layered network.

    Arguments:
    ----------
        weight_matrices: [w1, w2, ..., wn]
            list of weight matrices defining the connectivity between layers;
            each weight matrix is a 2-D ndarray with rows indexing source and columns indexing targets;
            the number of sources has to match the number of targets in the last layer

        distance_between_layers: int

        distance_between_nodes: int

        layer_labels: [str1, str2, ..., strn+1]
            labels of layers

        **kwargs: passed to netgraph.draw()

    Returns:
    --------
        ax: matplotlib axis instance

    """
    nodes_per_layer = _get_nodes_per_layer(weight_matrices)

    node_positions = _get_node_positions(nodes_per_layer,
                                         distance_between_layers,
                                         distance_between_nodes)

    w = _combine_weight_matrices(weight_matrices, nodes_per_layer)

    ax = netgraph.draw(w, node_positions, draw_arrows=True, **kwargs)

    if not layer_labels is None:
        ax.set_xticks(distance_between_layers * np.arange(len(weight_matrices) + 1))
        ax.set_xticklabels(layer_labels)
        ax.xaxis.set_ticks_position('bottom')

    return ax


def _get_nodes_per_layer(weight_matrices):
    nodes_per_layer = []
    for w in weight_matrices:
        sources, targets = w.shape
        nodes_per_layer.append(sources)
    nodes_per_layer.append(targets)
    return nodes_per_layer


def _get_node_positions(nodes_per_layer,
                        distance_between_layers,
                        distance_between_nodes):
    x = []
    y = []
    for ii, n in enumerate(nodes_per_layer):
        x.append(distance_between_nodes * np.arange(0., n))
        y.append(ii * distance_between_layers * np.ones((n)))
    x = np.concatenate(x)
    y = np.concatenate(y)

    return {i: coords for i, coords in enumerate(np.c_[y, x])}


def _combine_weight_matrices(weight_matrices, nodes_per_layer):
    total_nodes = np.sum(nodes_per_layer)
    w = np.full((total_nodes, total_nodes), np.nan, np.float)

    a = 0
    b = nodes_per_layer[0]
    for ii, ww in enumerate(weight_matrices):
        w[a:a + ww.shape[0], b:b + ww.shape[1]] = ww
        a += nodes_per_layer[ii]
        b += nodes_per_layer[ii + 1]

    w[np.isnan(w)] = 0

    return w


def parse_layers(layers):
    weight_matrices = []
    node_labels = []

    # initialise sources
    sources = set(layers[0])

    for layer in layers[1:]:
        targets = layer.keys()

        w = np.full((len(sources), len(targets)), np.nan, np.float)
        for ii, s in enumerate(sources):
            for jj, t in enumerate(targets):
                try:
                    w[ii, jj] = layer[t][s]
                except KeyError:
                    pass

        weight_matrices.append(w)
        node_labels.append(sources)
        sources = targets

    node_labels.append(targets)
    node_labels = list(itertools.chain.from_iterable(node_labels))
    node_labels = dict(enumerate(node_labels))

    return weight_matrices, node_labels


def plot_layers(layers, layer_labels=None, ax=None):
    weight_matrices, node_labels = parse_layers(layers)
    if not layer_labels:
        layer_labels = list(map(str, range(len(layers))))
        layer_labels[0] = ""

    if ax is None:
        fig, ax = plt.subplots(1, 1, figsize=(18, 8))
    return plot_layered_network(weight_matrices,
                                layer_labels=layer_labels,
                                distance_between_layers=20,
                                distance_between_nodes=8,
                                ax=ax,
                                node_size=300,
                                node_edge_width=10,
                                node_labels=node_labels,
                                edge_width=50)
