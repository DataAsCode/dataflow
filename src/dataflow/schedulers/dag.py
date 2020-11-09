from collections import defaultdict
import networkx as nx


class DAG:
    flow_nodes = {}
    flow_edges = defaultdict(list)

    @staticmethod
    def flow_name(cls):
        if isinstance(cls, str):
            return cls

        return cls.__name__

    @staticmethod
    def register_flow(cls):
        DAG.flow_nodes[DAG.flow_name(cls)] = cls

    @staticmethod
    def register_dependency(flow_class, depends_on):
        DAG.flow_edges[DAG.flow_name(depends_on)].append(DAG.flow_name(flow_class))

    @staticmethod
    def build():
        G = nx.DiGraph()

        for flow_name, flow in DAG.flow_nodes.items():
            G.add_node(flow_name, flow=flow, finished=False)

        for flow_name, dependees in DAG.flow_edges.items():
            for dependent in dependees:
                G.add_edge(flow_name, dependent)

        return G

    @staticmethod
    def show():
        G = DAG.build()

    @staticmethod
    def run(self):
        G = DAG.build()
