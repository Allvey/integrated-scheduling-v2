#!E:\Pycharm Projects\Waytous
# -*- coding: utf-8 -*-
# @Time : 2021/7/26 14:35
# @Author : Opfer
# @Site :
# @File : path_plannner.py    
# @Software: PyCharm

import numpy
from settings import *
from static_data_process import *

load_area_uuid_to_index_dict, unload_area_uuid_to_index_dict, \
load_area_index_to_uuid_dict, unload_area_index_to_uuid_dict = build_work_area_uuid_index_map()

load_area_num, unload_area_num = len(load_area_uuid_to_index_dict), len(unload_area_uuid_to_index_dict)

class PathPlanner:
    def __init__(self):
        # 路线行驶成本
        self.rout_cost = np.array((unload_area_num, load_area_num))
        # 路段集合
        self.lane_set = {}
        # 车辆长度
        self.truck_length = 10

    def path_cost_generate(self, path_id):
        pass

    def lane_cost_generate(self, lane_id):
        # 道路长度
        lane_length = 100
        # 路段实际矿卡速度
        actual_speed = 20
        # 车辆自由行驶时的速度
        clear_speed = 25
        # 1. 计算阻塞时车辆密度
        truck_density = lane_length / self.truck_length
        # 2. 读取实际车流速度
        actual_speed = 20
        # 3. 计算路段阻塞程度
        lane_blockage = (1 - actual_speed / clear_speed) * truck_density

