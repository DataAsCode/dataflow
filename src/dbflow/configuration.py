import os
import json

from dbflow.schedule import Schedule


class StaticConfiguration:
    path = "./dbflow.conf"

    def __init__(self):
        self.conf = {
            "schedule": {"every": None, "at": None, "interval": None},
            "folder": "flows",
            "auth": {
                "file": [
                    os.path.expanduser(f"~{os.sep}.dbflow{os.sep}connections.json"),
                    os.path.expanduser(f"~{os.sep}.pandas_db{os.sep}connections.json")
                ],
                "env": ["mf_config"]
            }
        }

        self.load_from_disc()

    @property
    def schedule(self):
        return self.conf["schedule"]

    @property
    def auth(self):
        auth_info = {}
        for key, values in self.conf["auth"].items():
            if key == "file":
                for value in values:
                    if os.path.exists(value):
                        auth_info[key] = value
                        break
                else:
                    auth_info["file"] = None
            if key == "env":
                for value in values:
                    if os.getenv(value):
                        auth_info[key] = os.getenv(value)
                        break
                else:
                    auth_info["env"] = ""

        return auth_info

    def load_from_disc(self):
        def decoder(key, obj):
            if key == "schedule":
                return Schedule.from_json(obj)

            return obj

        if not os.path.exists(self.path):
            self.load_to_disc()

        self.conf.update({key: decoder(key, value) for key, value in json.load(open(self.path)).items()})

    def load_to_disc(self):
        def encoder(obj):
            if isinstance(obj, Schedule):
                return dict(obj)

            return obj

        with open(self.path, "w") as output:
            json.dump(self.conf, output, default=encoder)

    def __call__(self, **kwargs):
        for item, value in kwargs.items():
            self.conf[item] = value

        self.load_to_disc()

        return self

    def __getitem__(self, item):
        return self.conf.get(item)


Configuration = StaticConfiguration()
