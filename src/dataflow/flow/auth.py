from metaflow import FlowSpec, IncludeFile
import os

from pandasdb.connections import PostgresConnection, RedshiftConnection
import json

CONFIG_FILE = os.path.expanduser(f"~{os.sep}.dataflow{os.sep}connections.json")
if not os.path.exists(CONFIG_FILE):
    CONFIG_FILE = os.path.expanduser(f"~{os.sep}.pandas_db{os.sep}connections.json")


class AuthorizedFlow(FlowSpec):
    auth_file = IncludeFile('auth_file', is_text=True, help='My input', default=CONFIG_FILE)

    def __init__(self, *args, **kwargs):
        FlowSpec.__init__(self, *args, **kwargs)

    @property
    def databases(self):
        connections = {name: self.to_db(name, info) for name, info in json.loads(self.auth_file).items()}
        return type("Databases", (), connections)

    def to_db(self, name, info):
        if "name" not in info:
            info["name"] = name

        if info["type"] == "POSTGRES":
            return PostgresConnection(**info)
        else:
            return RedshiftConnection(**info)
