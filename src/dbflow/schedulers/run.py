from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import sleep


class Job:
    def __init__(self, name, runnable, condition):
        self.name = name
        self._run = runnable
        self.done = False
        self.condition = condition

    def start(self):
        while not self.condition():
            sleep(1)

        print("Running", self.name)
        self._run()
        self.done = True

    def __bool__(self):
        return self.condition() and self.done


class DependencyThreadPool:
    def __init__(self, num_threads=20):
        self.job_broker = {}
        self.futures = {}
        self.pool = ThreadPoolExecutor(num_threads)

    def submit(self, name, runnable, dependencies=None):
        def condition(deps):
            if not deps:
                return True

            dependencies_done = [bool(self.job_broker.get(dep, False)) for dep in deps]
            return all(dependencies_done)

        job = Job(name, runnable, partial(condition, dependencies))
        self.futures[job.name] = self.pool.submit(job.start)
        self.job_broker[name] = job

    def __enter__(self):
        return self

    def __iter__(self):
        for name, future in self.futures.items():
            yield name, future.result()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.wait()

    def wait(self):
        list(self)
        while not all(self.job_broker.values()):
            sleep(1)
