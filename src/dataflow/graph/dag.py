import networkx as nx
from dataflow.graph.plot import plot_graph


class DAG:
    ROOT = "<ROOT>"
    flow_graph = nx.DiGraph()
    table_graph = nx.DiGraph()

    flows = {}

    @staticmethod
    def _add(graph, name, dependencies, module=None):
        if name not in graph.nodes:
            graph.add_node(DAG.flow_name(name), module=module)

        if not dependencies:
            graph.add_edge(DAG.ROOT, DAG.flow_name(name))
        else:
            for dependency in dependencies:
                if not graph.has_edge(DAG.flow_name(dependency), DAG.flow_name(name)):
                    graph.add_edge(DAG.flow_name(dependency), DAG.flow_name(name))

    @staticmethod
    def flow_execution():
        all_flows = set(DAG.flow_graph.nodes)

        seen_flows = set()
        for name in DAG.flow_graph.successors(DAG.ROOT):
            if name in all_flows and name != DAG.ROOT:
                seen_flows.add(name)
                yield name, []

        missing_streams = [name for name in all_flows if name not in seen_flows and name != DAG.ROOT]
        while missing_streams:
            for name in missing_streams:
                dependencies = [dependency for dependency in DAG.flow_graph.predecessors(name) if
                                dependency != DAG.ROOT]
                if all([dependency in seen_flows for dependency in dependencies]):
                    seen_flows.add(name)
                    yield name, dependencies

            missing_streams = [node for node in DAG.flow_graph.nodes if node not in seen_flows and node != DAG.ROOT]

    @staticmethod
    def add_flow(cls, dependencies, module):
        if dependencies is None:
            dependencies = []

        name = DAG.flow_name(cls)
        DAG.flows[name] = cls

        DAG._add(DAG.flow_graph, name, dependencies, module)

    @staticmethod
    def add_table_output(class_name, output_tables, module):
        if not isinstance(output_tables, (list, tuple, set)):
            output_tables = [output_tables]

        DAG._add(DAG.table_graph, class_name, output_tables, module)

    @staticmethod
    def add_table_input(class_name, input_tables, module):
        if not isinstance(input_tables, (list, tuple, set)):
            input_tables = [input_tables]

        for table_name in input_tables:
            DAG._add(DAG.table_graph, table_name, [class_name], module)

    @staticmethod
    def flow_name(cls):
        if isinstance(cls, str):
            return cls

        return cls.__name__

    @staticmethod
    def plot_flows():
        return plot_graph(DAG.flow_graph, DAG.ROOT)
