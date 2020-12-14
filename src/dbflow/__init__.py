# -*- coding: utf-8 -*-
from pkg_resources import get_distribution, DistributionNotFound

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = get_distribution(dist_name).version
except DistributionNotFound:
    __version__ = 'unknown'
finally:
    del get_distribution, DistributionNotFound


from dbflow.flow.data_flow import DataFlow
from dbflow.wrappers import register_flow, step, register_output_table, register_input_table
from dbflow.utils import load_all_flows
import schedule as schedule_flow