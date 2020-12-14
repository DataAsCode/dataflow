import inspect
from typing import List

from dbflow.graph.dag import DAG

from metaflow import step as meta_step
import functools


def step(func):
    # Ensure the function is properly wrapped by the Metaflow step function before moving on
    meta_func = meta_step(func)

    # Wrap the function in another function to enable call-time logic
    def _decorate(meta_func):
        @functools.wraps(meta_func)
        def wrapped_function(self, *args, **kwargs):
            return meta_func(self, *args, **kwargs)

        return wrapped_function

    # Return the decorated function
    return _decorate(meta_func)


def register_flow(cls=None, depends_on: List[str] = None):
    if cls is None:
        return functools.partial(register_flow, depends_on=depends_on)

    DAG.add_flow(cls, depends_on, cls.__module__)

    @functools.wraps(cls)
    def wrapped_function(*args, **kwargs):
        obj = cls(*args, **kwargs)

        return obj

    return wrapped_function


def register_output_table(database_ref, table_name, *columns):
    try:
        stack = inspect.stack()
        class_name = stack[1][0].f_locals.get("__qualname__")
        module = stack[1][0].f_locals.get("__module__")
        DAG.add_table_output(class_name, [f"{database_ref}.{table_name}"], module=module)
    except:
        pass

    def schema_ref(self):
        from dbflow.schema import TableSchema
        database = getattr(self.databases(), database_ref)
        return TableSchema(database, table_name, *columns)

    return schema_ref


def register_input_table(database_ref, table_name):
    try:
        stack = inspect.stack()
        class_name = stack[1][0].f_locals.get("__qualname__")
        module = stack[1][0].f_locals.get("__module__")
        DAG.add_table_input(class_name, [f"{database_ref}.{table_name}"], module)
    except:
        pass

    def table_ref(self):
        database = getattr(self.databases(), database_ref)()
        return getattr(database, table_name)

    return table_ref
