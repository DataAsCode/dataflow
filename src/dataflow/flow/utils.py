from typing import List

from dataflow.cache import Cache

import functools


def register_flow(flow_group: str, group_rank=999, depends_on: List[str] = None):
    """
    Add a flow to the overall DAG.

    :param cls:
    :param flow_group:
    :param group_rank:
    :param flow_parent:
    :return:
    """

    def _decorate(cls):
        Cache.register_flow(cls)
        Cache.register_flow_group(flow_group, depends_on, group_rank)

        @functools.wraps(cls)
        def wrapped_function(*args, **kwargs):
            cls(*args, **kwargs)

        return wrapped_function

    return _decorate


def schedule_flow(schedule):
    """
    Schedule a flow independently of the overall DAG. A scheduled flow cannot be
    combined with any other flows, since consistent runtime behaviour cannot be
    enforced.

    :param cls:
    :param schedule:
    :return:
    """

    def _decorate(cls):
        # Add the flow to a schedule independently of the DAG
        Cache.register_schedule(cls, schedule)

        @functools.wraps(cls)
        def wrapped_function(*args, **kwargs):
            cls(*args, **kwargs)

        return wrapped_function

    return _decorate
