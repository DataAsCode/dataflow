import csv
import inspect
import subprocess
from io import StringIO
from dataflow.flow.auth import AuthorizedFlow
import pandas as pd
from datetime import datetime
from dataflow.flow.db import DB


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

    @staticmethod
    def persist(database, table_name, df: pd.DataFrame, if_exists):
        now = datetime.now()
        df["_calculated"] = now

        if if_exists == "replace":
            DB.replace(database, table_name, df)
        else:
            DB.append(database, table_name, df)

    @staticmethod
    def replace(database, table_name, df: pd.DataFrame):

        DataFlow.persist(database, table_name, df, if_exists="replace")

    @staticmethod
    def append(database, table_name, df: pd.DataFrame):
        DataFlow.persist(database, table_name, df, if_exists="append")
