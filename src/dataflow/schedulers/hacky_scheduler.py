import subprocess
import time

from dataflow.cache import Cache
import schedule
from threading import Thread
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
from functools import partial


class HackyScheduler:
    def __init__(self, connection_info: dict, default_schedule):
        self.connections = connection_info
        self.default_schedule = default_schedule
        self.cache = Cache()

        self.pool = ThreadPoolExecutor(mp.cpu_count())

        self.async_queue = Queue()
        self.waiter = Thread(target=self.wait)
        self.waiter.start()

    def wait(self):
        while True:
            job = self.async_queue.get()
            job.result()


    def async_flow(self, cls):
        job = self.pool.submit(cls.hacky_run, (self.connections,))
        self.async_queue.put(job)
        print(f"Scheduled {cls.__name__}")

    def run(self):
        for name, cls in self.cache.data_flows.items():
            # Run the flow right away
            self.async_flow(cls)

            # Schedule it to continue to run
            self.schedule(cls)

        # Keep serving until interrupted
        self.serve()

    def schedule(self, cls):
        schd = self.cache.get_schedule(cls)
        schd = schd if schd is not None else self.default_schedule
        schd.do(partial(self.async_flow, cls=cls))

    @staticmethod
    def serve():
        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except KeyboardInterrupt:
                break
