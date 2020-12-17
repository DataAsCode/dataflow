import inspect
import json

import subprocess

from dbflow.configuration import Configuration
from metaflow import FlowSpec as MetaFlowSpec, Parameter, IncludeFile
from pandasdb.sql.config import Databases


class FlowSpec(MetaFlowSpec):
    auth_file = IncludeFile('auth_file', is_text=True, help='My input', default=Configuration.auth["file"])
    auth_env = Parameter('auth_env', type=str, default=Configuration.auth["env"])

    def databases(self):
        for auth_type in [self.auth_file, self.auth_env]:
            try:
                return Databases(connections=json.loads(auth_type))
            except:
                continue
        else:
            raise AttributeError("No authentication provided")

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
