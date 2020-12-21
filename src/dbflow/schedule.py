import time


class Schedule:
    def __init__(self, every=None, interval=None, at=None):
        self.config = {
            "every": every,
            "at": at,
            "interval": interval
        }

    @property
    def sched(self):
        import schedule
        sched = schedule

        every, interval, at = self.config["every"], self.config["interval"], self.config["at"]

        if interval is not None:
            sched = sched.every(interval)
        else:
            sched = sched.every()

        if every is not None:
            sched = getattr(sched, every)

        if at is not None:
            sched = sched.at(at)

        return sched

    def do(self, func):
        self.sched.do(func)
        return self

    @staticmethod
    def wait():
        import schedule
        while True:
            schedule.run_pending()
            time.sleep(1)

    @staticmethod
    def from_json(data):
        return Schedule(**data)

    def __iter__(self):
        return iter(self.config)
