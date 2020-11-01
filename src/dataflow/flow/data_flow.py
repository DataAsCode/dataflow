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

    def persist(self, database, table_name, df: pd.DataFrame, if_exists):
        pass
        # df["_calculated_at"] = datetime.now()
        # df["_calculated_by"] = type(self).__name__
        #
        # if if_exists == "replace":
        #     DB.replace(database, table_name, df)
        # else:
        #     DB.append(database, table_name, df)

    def replace(self, database, table_name, df: pd.DataFrame):
        self.persist(database, table_name, df, if_exists="replace")

    def append(self, database, table_name, df: pd.DataFrame):
        self.persist(database, table_name, df, if_exists="append")
