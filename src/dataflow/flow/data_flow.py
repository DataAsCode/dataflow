import functools
import inspect
import json
import subprocess
from typing import List, Optional
import schedule
import schedule as schedule
from dataflow.cache import Cache
from dataflow.connection import Connection
from dataflow.flow.auth import AuthorizedFlow
from dataflow.utils import optional_arg_decorator
from metaflow import FlowSpec, Parameter, JSONType


class DataFlow(AuthorizedFlow):
    connections = Parameter('connections', help='DB Connection Params', type=JSONType, default='{}')

    @classmethod
    def hacky_run(cls, connections):
        cmd = [
            'python',
            inspect.getfile(cls),  # The name of the file to be run
            'run',
            '--connections',
            json.dumps({})
        ]
        print(subprocess.run(cmd, capture_output=True))

