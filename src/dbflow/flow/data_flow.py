# pylint: disable=no-member
# pylint: disable=E1101
import inspect
import subprocess

from metaflow import FlowSpec, IncludeFile
import os
from functools import lru_cache
from pandasdb.sql.config import Databases
import json

CONFIG_FILE = os.path.expanduser(f"~{os.sep}.dbflow{os.sep}connections.json")
if not os.path.exists(CONFIG_FILE):
    CONFIG_FILE = os.path.expanduser(f"~{os.sep}.pandas_db{os.sep}connections.json")


class DataFlow(FlowSpec):
    auth_file = IncludeFile('auth_file', is_text=True, help='My input', default=CONFIG_FILE)

    @lru_cache
    def _all_connections(self):
        return json.loads(self.auth_file)

    def databases(self):
        return Databases(connections=self._all_connections())

    @classmethod
    def hacky_run(cls):
        cmd = [
            'python',
            inspect.getfile(cls),  # The name of the file to be run
            '--no-pylint',
            'run',
        ]
        result = subprocess.run(cmd, capture_output=True)
        return result.returncode == 0
