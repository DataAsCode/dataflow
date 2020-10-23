from typing import List, Dict

import schedule

from dataflow.utils import camel_to_snake


class Cache:
    data_flows = {}
    flow_schedules = {}

    @staticmethod
    def register_flow(cls):
        Cache.data_flows[camel_to_snake(cls.__name__)] = cls

    @staticmethod
    def register_schedule(cls, schedule):
        Cache.flow_schedules[camel_to_snake(cls.__name__)] = schedule

    @staticmethod
    def get_schedule(cls):
        return Cache.flow_schedules.get(camel_to_snake(cls.__name__))
