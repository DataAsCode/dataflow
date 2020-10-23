from typing import List

from dataflow.cache import Cache

import functools


def register_flow(cls=None, connections: List[str] = [], schedule=None):
    def _decorate(cls):
        Cache.register_flow(cls)

        if schedule:
            Cache.register_schedule(cls, schedule)

        if connections:
            setattr(cls, "available_connections", connections)

        @functools.wraps(cls)
        def wrapped_function(*args, **kwargs):
            cls(*args, **kwargs)

        return wrapped_function

    if cls:
        # Return the decorated function
        return _decorate(cls)

    return _decorate
