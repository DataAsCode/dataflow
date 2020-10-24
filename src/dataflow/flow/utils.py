from typing import List

from dataflow.cache import Cache

import functools


def register_flow(cls, flow_group: str, group_rank=-1, flow_parent=None, schedule=None):
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

    return _decorate(cls)
