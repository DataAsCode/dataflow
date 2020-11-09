import csv
import inspect
import subprocess
from io import StringIO
from dataflow.flow.auth import AuthorizedFlow
import pandas as pd
from datetime import datetime


class DataFlow(AuthorizedFlow):

    @classmethod
    def hacky_run(cls):
        cmd = [
            'python',
            inspect.getfile(cls),  # The name of the file to be run
            '--no-pylint',
            'run',
            '--connections'
        ]
        print(subprocess.run(cmd, capture_output=True))
