from sys import getsizeof

import psycopg2
import pandas as pd
from sqlalchemy import Table, MetaData
import numpy as np
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class TableSchema:
    metadata = MetaData()

    def __init__(self, name, *columns):
        self.name = name
        self._dtypes = {column.name: column.type for column in columns}
        self._table = Table(name, TableSchema.metadata, *columns)

    def replace(self, database, df: pd.DataFrame):
        self._table.schema = database.schema
        self._table.drop(database.engine(), checkfirst=True)
        self._table.create(database.engine())

        # Change all NaN values to None in to store it properly in the database
        df = df.astype(object).where(pd.notnull(df), None)

        # self._insert_alt(database, df)
        self.insert(database, self.name, df)

    # def _insert_alt(self, database, df):
    #     with database.connect() as conn:
    #         conn = conn.execution_options(schema_translate_map={None: database.schema})
    #         conn.execute(self._table.insert(), df.to_dict(orient="records"))

    @staticmethod
    def insert(database, name, df):
        def generate_chunk(tuples):
            # SQL query to execute
            value_shema = ', '.join(['%s' for _ in range(len(df.columns))])

            cursor = database.engine().raw_connection().cursor()
            values = b','.join([b"(" + cursor.mogrify(value_shema, x) + b")" for x in tuples])
            return f"INSERT INTO {database.schema}.{name}({cols}) VALUES " + values.decode('utf-8')

        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))

        value_tuples = [tuple(x) for x in df.to_numpy()]  # [:1000]

        with database.connect() as connection:
            connection = connection.execution_options(schema_translate_map={None: database.schema})

            optimum_query_size = 8e+6
            orig_size = getsizeof(generate_chunk(value_tuples))

            if orig_size > optimum_query_size:
                num_chunks = max(orig_size // optimum_query_size, 1)
            else:
                num_chunks = 1

            queries = list(map(generate_chunk, np.array_split(value_tuples, num_chunks)))
            for query in queries:
                try:
                    connection.execute(query)
                except (Exception, psycopg2.DatabaseError) as error:
                    print("Error: %s" % error)
