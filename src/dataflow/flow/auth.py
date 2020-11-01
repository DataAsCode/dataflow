# pylint: disable=no-member
# pylint: disable=E1101

from metaflow import FlowSpec, IncludeFile
import os
from functools import partial, lru_cache
from pandasdb.sql.config import Databases
import json

CONFIG_FILE = os.path.expanduser(f"~{os.sep}.dataflow{os.sep}connections.json")
if not os.path.exists(CONFIG_FILE):
    CONFIG_FILE = os.path.expanduser(f"~{os.sep}.pandas_db{os.sep}connections.json")


class AuthorizedFlow(FlowSpec):
    auth_file = IncludeFile('auth_file', is_text=True, help='My input', default=CONFIG_FILE)

    @lru_cache
    def _all_connections(self):
        return json.loads(self.auth_file)

    @property
    def databases(self):
        return Databases(connections=self._all_connections())
