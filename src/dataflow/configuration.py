from dataflow import schedule_flow


class Configuration:
    def __init__(self, **kwargs):
        self.conf = {
            "schedule": schedule_flow.every(8).hours,
            "folder": "flows"
        }

        for item, value in kwargs.items():
            self.conf[item] = value

    def __getitem__(self, item):
        return self.conf.get(item)
