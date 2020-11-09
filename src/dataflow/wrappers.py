from typing import List

from dataflow.schedulers.dag import DAG

from metaflow import step as meta_step
import functools


def step(func):
    # Ensure the function is properly wrapped by the Metaflow step function before moving on
    meta_func = meta_step(func)

    # Wrap the function in another function to enable call-time logic
    def _decorate(meta_func):
        @functools.wraps(meta_func)
        def wrapped_function(*args, **kwargs):
            meta_func(*args, **kwargs)

        return wrapped_function

    # Return the decorated function
    return _decorate(meta_func)


def register_flow(cls=None, depends_on: List[str] = None):
    if cls is None:
        return functools.partial(register_flow, depends_on=depends_on)

    def _decorate(cls):
        DAG.register_flow(cls)
        for name in depends_on:
            DAG.register_dependency(cls, name)

        @functools.wraps(cls)
        def wrapped_function(*args, **kwargs):
            cls(*args, **kwargs)

        return wrapped_function

    return _decorate

def register_output_table(cls=None):
    if cls is None:
        return functools.partial(register_output_table)

    def _decorate(cls):
        @functools.wraps(cls)
        def wrapped_function(*args, **kwargs):
            cls(*args, **kwargs)

        return wrapped_function

    return _decorate
