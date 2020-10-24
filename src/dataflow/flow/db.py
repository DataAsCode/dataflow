from datetime import datetime

import pandas as pd
import os
from io import StringIO

import psycopg2
import psycopg2.extras as extras

import pandas as pd
import numpy as np

type_map = {
    int: "bigint",

    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",

    str: "VARCHAR",
    np.int64: "BIGINT",
    datetime: "DATE",
    np.bool_: "BOOLEAN",
    np.datetime64: "DATE"
}


class DB:

    @staticmethod
    def append(database, table_name, df: pd.DataFrame):
        try:
            DB.create_table(database, table_name, df)
        except:
            pass

        DB.insert(database, table_name, df)

    @staticmethod
    def replace(database, table_name, df: pd.DataFrame):
        DB.drop_table(database, table_name)
        DB.create_table(database, table_name, df)
        DB.insert(database, table_name, df)

    @staticmethod
    def drop_table(database, name):
        with database.conn as conn:
            with conn.cursor() as c:
                c.execute(f"DROP TABLE IF EXISTS {database.schema}.{name} CASCADE")
                conn.commit()

    @staticmethod
    def create_table(database, name, df):

        columns = df.columns
        types = [type_map[type(df.dropna()[c].values[0])] for c in df.dropna().columns]
        nullable = [sum(df[c].isna()) > 0 for c in df.columns]

        sql_cols = []
        for column, dtype, is_nullable in zip(columns, types, nullable):
            sql_cols.append(f"{column} {dtype} {'not null' if not is_nullable else ''}")

        sql = """
        CREATE TABLE {table_name} (
        {columns}
        )
        """.format(table_name=f"{database.schema}.{name}", columns=",\n".join(sql_cols))
        print(sql)

        with database.conn as conn:
            with conn.cursor() as c:
                c.execute(sql)
                conn.commit()

    @staticmethod
    def insert(database, name, df):
        """
        Using psycopg2.extras.execute_batch() to insert the dataframe
        """
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]

        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))

        # SQL quert to execute
        query = f"INSERT INTO {database.schema}.{name}({cols}) VALUES({', '.join(['%s' for _ in range(len(df.columns))])})"
        print(query)
        # Copy the string buffer to the database, as if it were an actual file
        with database.conn as conn:
            with conn.cursor() as c:
                try:
                    extras.execute_batch(c, query, tuples, max(100, len(df) / 10))
                    conn.commit()
                except (Exception, psycopg2.DatabaseError) as error:
                    print("Error: %s" % error)
                    conn.rollback()
                    c.close()
                    conn.commit()
