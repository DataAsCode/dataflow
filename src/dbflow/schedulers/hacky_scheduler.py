import time

from dbflow.configuration import Configuration
from dbflow.graph.dag import DAG
import matplotlib.pyplot as plt
import argparse
from dbflow import schedule_flow, load_all_flows

from dbflow.schedulers.run import DependencyThreadPool


class HackyScheduler:
    def __init__(self, configuration=None):
        self.configuration = Configuration(**configuration)
        self.setup()
        self.parse_args()

    def setup(self):
        load_all_flows(folder=self.configuration["folder"])

    def parse_args(self):
        parser = argparse.ArgumentParser(description='Process some integers.')

        parser.add_argument('--plot_tables', nargs="?", const=True, default=False, help='plot table graphs')
        parser.add_argument('--plot_flows', nargs="?", const=True, default=False, help='plot flow graphs')
        parser.add_argument('--plot', nargs="?", const=True, default=False, help='plot all graphs')
        parser.add_argument('--run', nargs="?", const=True, default=False, help='run the whole dbflow once')
        parser.add_argument('--serve', nargs="?", const=True, default=False,
                            help='serve the whole dbflow according to the schedule')
        args = parser.parse_args()

        if args.run:
            self.run()
        if args.serve:
            self.serve()
        if args.plot:
            self.plot_flows()
            self.plot_tables()
        elif args.plot_tables:
            self.plot_tables()
        elif args.plot_flows:
            self.plot_flows()

    @staticmethod
    def run():
        with DependencyThreadPool() as pool:
            for name, dependencies in DAG.flow_execution():
                pool.submit(name, DAG.flows[name].hacky_run, dependencies)

    def serve(self):
        self.configuration.schedule.do(self.run)
        while True:
            schedule_flow.run_pending()
            time.sleep(1)

    @staticmethod
    def plot_flows():
        ax = DAG.plot_flows()
        plt.savefig('flows.png')

    def plot_tables(self):
        pass
