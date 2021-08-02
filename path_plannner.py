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
from settings import *

load_area_uuid_to_index_dict, unload_area_uuid_to_index_dict, \
load_area_index_to_uuid_dict, unload_area_index_to_uuid_dict = build_work_area_uuid_index_map()

park_uuid_to_index_dict, park_index_to_uuid_dict = build_park_uuid_index_map()

load_area_num, unload_area_num = len(load_area_uuid_to_index_dict), len(unload_area_uuid_to_index_dict)

truck_uuid_to_name_dict, truck_name_to_uuid_dict = build_truck_uuid_name_map()

M = 1000000


class PathPlanner:
    def __init__(self):
        # 路线行驶成本
        self.rout_cost = np.array((unload_area_num, load_area_num))
        # 路段集合
        self.lane_set = {}
        # 车辆长度(暂)
        self.truck_length = 113
        # 装载区数量
        self.num_of_load_area = len(set(update_load_area()))
        # 卸载区数量
        self.num_of_unload_area = len(set(update_unload_area()))
        # 备停区数量
        self.num_of_park_area = len(set(update_park_area()))
        # 路网信息
        self.walk_time_to_load_area = np.full((self.num_of_unload_area, self.num_of_load_area), M)
        self.walk_time_to_unload_area = np.full((self.num_of_unload_area, self.num_of_load_area), M)
        # 路网信息（备停区）
        self.walk_time_park = np.full((self.num_of_park_area, self.num_of_load_area), M)

    def path_cost_generate(self, load_area_id, unload_area_id, is_park):

        to_unload_blockage_cost = 0
        to_load_blockage_cost = 0
        to_unload_cost = 0
        to_load_cost = 0

        if is_park:
            path = session_postgre.query(WalkTimePark).filter_by(park_area_id=unload_area_id,
                                                                 load_area_id=load_area_id).first()

            for lane_id in path.park_load_lanes:
                to_load_blockage_cost = to_load_blockage_cost + self.lane_cost_generate(lane_id)

            to_load_cost = to_load_blockage_cost + path.park_load_distance
        else:
            path = session_postgre.query(WalkTime).filter_by(load_area_id=load_area_id,
                                                             unload_area_id=unload_area_id).first()

            for lane_id in path.to_unload_lanes:
                to_unload_blockage_cost = to_unload_blockage_cost + self.lane_cost_generate(lane_id)

            for lane_id in path.to_load_lanes:
                to_load_blockage_cost = to_load_blockage_cost + self.lane_cost_generate(lane_id)

            print(to_unload_blockage_cost, to_load_blockage_cost)

            to_unload_cost = to_unload_blockage_cost + path.to_unload_distance
            to_load_cost = to_load_blockage_cost + path.to_load_distance

        return to_unload_cost, to_load_cost

    def lane_cost_generate(self, lane_id):

        lane = session_postgre.query(Lane).filter_by(Id=lane_id).first()

        # 道路长度
        lane_length = lane.Length
        # 车辆自由行驶时的速度
        clear_speed = lane.MaxSpeed

        # 1. 计算阻塞时车辆密度
        truck_density = lane_length / self.truck_length
        # 2. 读取实际车流速度(暂)
        actual_speed = clear_speed
        # 3. 计算路段阻塞程度
        lane_blockage = (1 - actual_speed / clear_speed) * truck_density

        return lane_blockage

    def walk_cost(self):
        for walk_time in session_postgre.query(WalkTime).all():
            unload_area_index = unload_area_uuid_to_index_dict[str(walk_time.unload_area_id)]
            load_area_index = load_area_uuid_to_index_dict[str(walk_time.load_area_id)]
            self.walk_time_to_load_area[unload_area_index][load_area_index], \
            self.walk_time_to_unload_area[unload_area_index][load_area_index] = \
                self.path_cost_generate(walk_time.load_area_id, walk_time.unload_area_id, False)

        for walk_time_park in session_postgre.query(WalkTimePark).all():
            park_area_index = park_uuid_to_index_dict[str(walk_time_park.park_area_id)]
            load_area_index = load_area_uuid_to_index_dict[str(walk_time_park.load_area_id)]
            _, self.walk_time_park[park_area_index][load_area_index] = \
                self.path_cost_generate(walk_time_park.load_area_id, walk_time_park.park_area_id, True)

        print(self.walk_time_to_unload_area)
        print(self.walk_time_to_load_area)
        print(self.walk_time_park)

    def update_truck_speed(self):
        truck_speed_dict = {}
        device_name_set = redis2.keys()
        for item in device_name_set:
            item = item.decode(encoding='utf-8')
            json_value = json.loads(redis2.get(item))
            device_type = json_value.get('type')
            if device_type == 1:
                truck_speed = json_value.get('speed')
                truck_speed_dict[truck_name_to_uuid_dict[item]] = truck_speed

        return truck_speed_dict


planner = PathPlanner()

planner.walk_cost()

planner.update_truck_speed()
