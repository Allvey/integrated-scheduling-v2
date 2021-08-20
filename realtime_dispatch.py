#!E:\Pycharm Projects\Waytous
# -*- coding: utf-8 -*-
# @Time : 2021/7/21 16:45
# @Author : Opfer
# @Site :
# @File : realtime_dispatch.py
# @Software: PyCharm


# 实时调度模块


from traffic_flow.traffic_flow_planner import *
from static_data_process import *
from para_config import *
from settings import *


# 卸载设备类
class DumpInfo(WalkManage):
    def __init__(self):
        # 卸载设备数量
        self.dynamic_dump_num = len(dynamic_dump_set)
        # 目标产量
        self.dump_target_mass = np.zeros(self.dynamic_dump_num)
        # 实际真实产量
        self.cur_dump_real_mass = np.zeros(self.dynamic_dump_num)
        # # 预计产量（包含正在驶往目的地的矿卡载重）
        # self.pre_dump_real_mass = copy.deepcopy(self.cur_dump_real_mass)
        # # 模拟实际产量（防止调度修改真实产量）
        # self.sim_dump_real_mass = np.zeros(self.dynamic_dump_num)
        # # 真实设备可用时间
        # self.cur_dump_ava_time = np.zeros(self.dynamic_dump_num)
        # # 模拟各设备可用时间（防止调度修改真实产量）
        # self.sim_dump_ava_time = np.zeros(self.dynamic_dump_num)
        # 用于动态调度的卸载设备集合
        self.dynamic_dump_set = []
        # 开始时间
        self.start_time = datetime.now()
        # 卸载时间
        self.unloading_time = np.zeros(self.dynamic_dump_num)
        # 入场时间
        self.entrance_time = np.zeros(self.dynamic_dump_num)
        # 出场时间
        self.exit_time = np.zeros(self.dynamic_dump_num)

    def get_unloading_time(self):
        return self.unloading_time

    def get_dump_num(self):
        return self.dynamic_dump_num

    def get_dump_target_mass(self):
        return self.dump_target_mass

    def get_dump_actual_mass(self):
        return self.cur_dump_real_mass

    def get_dynamic_dump_set(self):
        return self.dynamic_dump_set

    # 更新卸载设备卸载时间
    def update_dump_unloadtime(self):
        self.unloading_time = np.zeros(self.dynamic_dump_num)

        for dump_id in self.dump_uuid_to_index_dict.keys():
            ave_unload_time = 0
            unload_count = 0
            try:
                for query in session_mysql.query(JobRecord.start_time, JobRecord.end_time). \
                        join(Equipment, JobRecord.equipment_id == Equipment.equipment_id). \
                        filter(Equipment.id == dump_id, JobRecord.end_time != None). \
                        order_by(JobRecord.start_time.desc()).limit(10):
                    ave_unload_time = ave_unload_time + float(
                        (query.end_time - query.start_time) / timedelta(hours=0, minutes=1, seconds=0))
                    unload_count = unload_count + 1
                self.unloading_time[self.dump_uuid_to_index_dict[dump_id]] = ave_unload_time / unload_count
            except Exception as es:
                logger.error(f'卸载设备 {dump_id} 卸载时间信息缺失, 已设为默认值(1min)')
                logger.error(es)
                self.unloading_time[self.dump_uuid_to_index_dict[dump_id]] = 5.00
        # print("average_unload_time: ", self.unloading_time[self.dump_uuid_to_index_dict[dump_id]])

    # 更新卸载设备出入时间
    def update_dump_entrance_exit_time(self):
        self.entrance_time = np.zeros(self.dynamic_dump_num)
        self.exit_time = np.zeros(self.dynamic_dump_num)
        now = datetime.now().strftime('%Y-%m-%d')

        for dump_id in self.dump_uuid_to_index_dict.keys():
            try:
                for query in session_mysql.query(WorkRecord).filter(WorkRecord.equipment_id == dump_id, \
                        WorkRecord.work_day > now).first():
                    self.entrance_time[self.dump_uuid_to_index_dict[dump_id]] = float(query.load_entrance_time / query.load_entrance_count)
                    self.exit_time[self.dump_uuid_to_index_dict[dump_id]] = float(query.exit_entrance_time / query.exit_entrance_count)
            except Exception as es:
                logger.error(f'卸载设备 {dump_id} 出入场时间信息缺失, 已设为默认值(1min)')
                logger.error(es)
                self.entrance_time[self.dump_uuid_to_index_dict[dump_id]] = 0.50
                self.exit_time[self.dump_uuid_to_index_dict[dump_id]] = 0.50

    # 读取出入场时间
    def get_unloading_task_time(self):
        unloading_time = self.unloading_time

        dump_entrance_time = self.entrance_time

        dump_exit_time = self.exit_time

        unloading_task_time = unloading_time + dump_entrance_time + dump_exit_time

        return unloading_task_time

    # 更新卸载设备实际卸载量
    def update_actual_unload_thoughout(self):
        self.cur_dump_real_mass = np.zeros(self.dynamic_dump_num)
        now = datetime.now().strftime('%Y-%m-%d')
        for dump_id in self.dump_uuid_to_index_dict.keys():
            for query in session_mysql.query(LoadInfo). \
                    join(Equipment, LoadInfo.dump_id == Equipment.equipment_id). \
                    filter(Equipment.id == dump_id, LoadInfo.time > now). \
                    order_by(LoadInfo.time.desc()).all():
                # print("time:", query.time)
                # print("load_weight:", )
                self.cur_dump_real_mass[self.dump_uuid_to_index_dict[dump_id]] = \
                    self.cur_dump_real_mass[self.dump_uuid_to_index_dict[dump_id]] + query.load_weight

    def period_update(self):

        print("Dump update!")

        # 装载周期参数
        self.period_map_para_load()

        self.period_walk_para_load()

        # # 初始化卸载设备可用时间
        # self.cur_dump_ava_time = np.full(self.dynamic_dump_num,
        #                                    (datetime.now() - self.start_time) / timedelta(hours=0, minutes=1,
        #                                                                                   seconds=0))

        # 用于动态调度的卸载设备
        self.dynamic_dump_set = set(update_autodisp_dump())

        self.dynamic_dump_num = len(self.dynamic_dump_set)

        # 计算平均卸载时间
        self.update_dump_unloadtime()

        # 计算实时卸载量
        self.update_actual_unload_thoughout()

        # 卸载目标产量
        self.dump_target_mass = np.full(self.dynamic_dump_num, dump_target_mass)

        # # 同步虚拟卸载量
        # self.sim_dump_real_mass = copy.deepcopy(self.cur_dump_real_mass)

        # # 计算卸载设备预估产量
        # self.update_pre_unload_throughout()


# 挖机设备类
class ExcavatorInfo(WalkManage):
    def __init__(self):
        # 装载设备数量
        self.dynamic_excavator_num = len(dynamic_excavator_set)
        # 目标产量
        self.excavator_target_mass = np.zeros(self.dynamic_excavator_num)
        # 真实实际产量
        self.cur_excavator_real_mass = np.zeros(self.dynamic_excavator_num)
        # # 预计产量（包含正在驶往目的地的矿卡载重）
        # self.pre_excavator_real_mass = copy.deepcopy(self.cur_excavator_real_mass)
        # # 模拟实际产量(防止调度修改真实产量)
        # self.sim_excavator_real_mass = np.zeros(self.dynamic_excavator_num)
        # # 真实设备可用时间
        # self.cur_excavator_ava_time = np.zeros(self.dynamic_excavator_num)
        # # 模拟各设备可用时间(防止调度修改真实产量)
        # self.sim_excavator_ava_time = np.zeros(self.dynamic_excavator_num)
        # 用于动态调度的卸载设备集合
        self.dynamic_excavator_set = []
        # 开始时间
        self.start_time = datetime.now()
        # 装载时间
        self.loading_time = np.zeros(self.dynamic_excavator_num)
        # 入场时间
        self.entrance_time = np.zeros(self.dynamic_excavator_num)
        # 出场时间
        self.exit_time = np.zeros(self.dynamic_excavator_num)

    # def period_map_para_load(self):
    #     # 关系映射
    #     self.excavator_uuid_to_index_dict = device_map.excavator_uuid_to_index_dict
    #     self.dump_uuid_to_index_dict = device_map.dump_uuid_to_index_dict
    #     self.excavator_index_to_uuid_dict = device_map.excavator_index_to_uuid_dict
    #     self.dump_index_to_uuid_dict = device_map.dump_index_to_uuid_dict
    #
    #     self.dump_uuid_to_unload_area_uuid_dict = device_map.dump_uuid_to_unload_area_uuid_dict
    #     self.excavator_uuid_to_load_area_uuid_dict = device_map.excavator_uuid_to_load_area_uuid_dict
    #     self.excavator_index_to_load_area_index_dict = device_map.excavator_index_to_load_area_index_dict
    #     self.dump_index_to_unload_area_index_dict = device_map.dump_index_to_unload_area_index_dict
    #
    # def period_walk_para_load(self):
    #     self.truck_uuid_to_index_dict = device_map.truck_uuid_to_index_dict
    #     self.truck_index_to_uuid_dict = device_map.truck_index_to_uuid_dict

    def get_loading_time(self):
        return self.loading_time

    def get_excavator_num(self):
        return self.dynamic_excavator_num

    def get_excavator_target_mass(self):
        return self.excavator_target_mass

    def get_excavator_actual_mass(self):
        return self.cur_excavator_real_mass

    def get_dynamic_excavator_set(self):
        return self.dynamic_excavator_set

    # 更新挖机装载时间
    def update_excavator_loadtime(self):
        self.loading_time = np.zeros(self.dynamic_excavator_num)

        for excavator_id in self.excavator_uuid_to_index_dict.keys():
            ave_load_time = 0
            load_count = 0
            try:
                for query in session_mysql.query(JobRecord.start_time, JobRecord.end_time). \
                        join(Equipment, JobRecord.equipment_id == Equipment.equipment_id). \
                        filter(Equipment.id == excavator_id, JobRecord.end_time != None). \
                        order_by(JobRecord.start_time.desc()).limit(10):
                    ave_load_time = ave_load_time + float(
                        (query.end_time - query.start_time) / timedelta(hours=0, minutes=1, seconds=0))
                    load_count = load_count + 1
                self.loading_time[self.excavator_uuid_to_index_dict[excavator_id]] = ave_load_time / load_count
            except Exception as es:
                logger.error(f'挖机 {excavator_id} 装载时间信息缺失, 已设为默认值(1min)')
                logger.error(es)
                self.loading_time[self.excavator_uuid_to_index_dict[excavator_id]] = 5.00

    # 更新挖机设备出入时间
    def update_excavator_entrance_exit_time(self):
        self.entrance_time = np.zeros(self.dynamic_excavator_num)
        self.exit_time = np.zeros(self.dynamic_excavator_num)
        now = datetime.now().strftime('%Y-%m-%d')

        for excavator_id in self.excavator_uuid_to_index_dict.keys():
            try:
                for query in session_mysql.query(WorkRecord).filter(WorkRecord.equipment_id == excavator_id, \
                                                                    WorkRecord.work_day > now).first():
                    self.entrance_time[self.excavator_uuid_to_index_dict[excavator_id]] = float(query.load_entrance_time / query.load_entrance_count)
                    self.exit_time[self.excavator_uuid_to_index_dict[excavator_id]] = float(query.exit_entrance_time / query.exit_entrance_count)
            except Exception as es:
                logger.error(f'挖机设备 {excavator_id} 出入场时间信息缺失, 已设为默认值(1min)')
                logger.error(es)
                self.entrance_time[self.excavator_uuid_to_index_dict[excavator_id]] = 0.50
                self.exit_time[self.excavator_uuid_to_index_dict[excavator_id]] = 0.50

    # 读取出入场时间
    def get_loading_task_time(self):
        loading_time = self.loading_time

        excavator_entrance_time = self.entrance_time

        excavator_exit_time = self.exit_time

        loading_task_time = loading_time + excavator_entrance_time + excavator_exit_time

        return loading_task_time

    # 更新挖机实际装载量
    def update_actual_load_throughout(self):
        self.cur_excavator_real_mass = np.zeros(self.dynamic_excavator_num)
        now = datetime.now().strftime('%Y-%m-%d')
        for excavator_id in self.excavator_uuid_to_index_dict.keys():
            # print(excavator_id)
            for query in session_mysql.query(LoadInfo). \
                    join(Equipment, LoadInfo.dump_id == Equipment.equipment_id). \
                    filter(Equipment.id == excavator_id, LoadInfo.time > now). \
                    order_by(LoadInfo.time.desc()).all():
                # print("time:", query.time)
                # print("load_weight:", )
                self.cur_excavator_real_mass[self.excavator_uuid_to_index_dict[excavator_id]] = \
                    self.cur_excavator_real_mass[self.excavator_uuid_to_index_dict[excavator_id]] + query.load_weight

    def period_update(self):

        print("Excavator update!")

        # 装载周期参数
        self.period_map_para_load()

        self.period_walk_para_load()

        # # 初始化挖机可用时间
        # self.cur_excavator_ava_time = np.full(self.dynamic_excavator_num,
        #                                    (datetime.now() - self.start_time) / timedelta(hours=0, minutes=1,
        #                                                                                   seconds=0))

        # 用于动态调度的挖机设备
        self.dynamic_excavator_set = set(update_autodisp_excavator())

        self.dynamic_excavator_num = len(self.dynamic_excavator_set)

        # 计算平均装载时间
        self.update_excavator_loadtime()

        # 计算实时装载量
        self.update_actual_load_throughout()

        # 挖机目标产量
        self.excavator_target_mass = np.full(self.dynamic_excavator_num, excavator_target_mass)

        # # 同步挖机虚拟装载量
        # self.sim_excavator_real_mass = copy.deepcopy(self.cur_excavator_real_mass)

        # # 计算卸载设备预估产量
        # self.update_pre_load_throughout()


# # 路网信息类
# class WalkManage(DeviceMap):
#     def __init__(self):
#         # 工作区和设备不具备一一对应关系, 为方便就计算, 算法维护两套路网: 面向路网和面向设备
#         # 行走时间(面向路网)
#         self.walk_time_to_load_area = np.full((unload_area_num, load_area_num), M)
#         self.walk_time_to_unload_area = np.full((unload_area_num, load_area_num), M)
#         # 行走时间(面向设备)
#         self.walk_time_to_excavator = np.full((len(set(update_autodisp_dump())), len(set(update_autodisp_excavator()))), M)
#         self.walk_time_to_dump = np.full((len(set(update_autodisp_dump())), len(set(update_autodisp_excavator()))), M)
#         # 备停区行走时间(面向路网)
#         self.walk_time_park_to_load_area = np.full((park_num, load_area_num), M)
#         # 备停区行走时间(面向设备)
#         self.walk_time_park_to_excavator = np.full((park_num, len(update_autodisp_excavator())), M)
#
#     def load(self):
#         # 关系映射
#         self.excavator_uuid_to_index_dict = device_map.excavator_uuid_to_index_dict
#         self.dump_uuid_to_index_dict = device_map.dump_uuid_to_index_dict
#         self.excavator_index_to_uuid_dict = device_map.excavator_index_to_uuid_dict
#         self.dump_index_to_uuid_dict = device_map.dump_index_to_uuid_dict
#
#         self.dump_uuid_to_unload_area_uuid_dict = device_map.dump_uuid_to_unload_area_uuid_dict
#         self.excavator_uuid_to_load_area_uuid_dict = device_map.excavator_uuid_to_load_area_uuid_dict
#         self.excavator_index_to_load_area_index_dict = device_map.excavator_index_to_load_area_index_dict
#         self.dump_index_to_unload_area_index_dict = device_map.dump_index_to_unload_area_index_dict
#
#         self.truck_uuid_to_index_dict = device_map.truck_uuid_to_index_dict
#         self.truck_index_to_uuid_dict = device_map.truck_index_to_uuid_dict
#
#     def get_walk_time_to_load_area(self):
#         return self.walk_time_to_load_area
#
#     def get_walk_time_to_unload_area(self):
#         return self.walk_time_to_unload_area
#
#     def get_walk_time_to_excavator(self):
#         return self.walk_time_to_excavator
#
#     def get_walk_time_to_dump(self):
#         return self.walk_time_to_dump
#
#     def get_walk_time_park_to_load_area(self):
#         return self.walk_time_park_to_load_area
#
#     def get_walk_time_park_to_excavator(self):
#         return self.walk_time_park_to_excavator
#
#     def update_walk_time(self):
#
#         self.load()
#
#         dump_num = len(set(update_autodisp_dump()))
#         excavator_num = len(set(update_autodisp_excavator()))
#
#         self.walk_time_to_excavator = np.full((dump_num, excavator_num), M)
#         self.walk_time_to_dump = np.full((dump_num, excavator_num), M)
#         self.walk_time_park_to_excavator = np.full((park_num, excavator_num), M)
#
#         # 计算路网行走时间
#         try:
#             # 处理距离
#             for item in session_postgre.query(WalkTime).all():
#                 load_area = str(item.load_area_id)
#                 unload_area = str(item.unload_area_id)
#                 load_area_index = load_area_uuid_to_index_dict[load_area]
#                 unload_area_index = unload_area_uuid_to_index_dict[unload_area]
#                 self.walk_time_to_load_area[unload_area_index][load_area_index] = float(
#                     60 / 1000 * item.to_load_distance / empty_speed)
#                 self.walk_time_to_unload_area[unload_area_index][load_area_index] = float(
#                     60 / 1000 * item.to_unload_distance / heavy_speed)
#         except Exception as es:
#             logger.error("路网信息异常")
#             logger.error(es)
#
#         # 计算设备路网行走时间
#         # try:
#
#         print(self.excavator_index_to_load_area_index_dict)
#
#         for i in range(dump_num):
#             for j in range(excavator_num):
#                 self.walk_time_to_excavator[i][j] = self.walk_time_to_load_area[self.dump_index_to_unload_area_index_dict[i]] \
#                     [self.excavator_index_to_load_area_index_dict[j]]
#                 self.walk_time_to_dump[i][j] = self.walk_time_to_unload_area[self.dump_index_to_unload_area_index_dict[i]] \
#                     [self.excavator_index_to_load_area_index_dict[j]]
#
#         # except Exception as es:
#         #     logger.error("设备路网信息异常异常")
#         #     logger.error(es)
#
#         try:
#             for item in session_postgre.query(WalkTimePark).all():
#                 load_area = str(item.load_area_id)
#                 park_area = str(item.park_area_id)
#                 load_area_index = load_area_uuid_to_index_dict[load_area]
#                 park_index = park_uuid_to_index_dict[park_area]
#                 self.walk_time_park_to_load_area[park_index][load_area_index] = 60 / 1000 * item.park_load_distance / empty_speed
#         except Exception as es:
#             logger.error("备停区路网信息异常")
#             logger.error(es)
#
#         try:
#             for i in range(park_num):
#                 for j in range(excavator_num):
#                     self.walk_time_park_to_excavator[i][j] = self.walk_time_park_to_load_area[i][
#                         self.excavator_index_to_load_area_index_dict[j]]
#         except Exception as es:
#             logger.error("备停区设备路网信息异常")
#             logger.error(es)


# 矿卡设备类
class TruckInfo(WalkManage):
    def __init__(self):
        # object fileds
        # self.walker = WalkManage()
        # 矿卡数量
        self.dynamic_truck_num = len(dynamic_truck_set)
        # 矿卡抵达卸载设备时间
        self.cur_truck_reach_dump = np.zeros(self.dynamic_truck_num)
        # 矿卡抵达挖机时间
        self.cur_truck_reach_excavator = np.zeros(self.dynamic_truck_num)
        # 用于动态派车的矿卡集合
        self.dynamic_truck_set = []
        # 矿卡最后装载/卸载时间
        self.last_load_time = {}
        self.last_unload_time = {}
        # 相对矿卡最后装载/卸载时间
        self.relative_last_load_time = {}
        self.relative_last_unload_time = {}
        # 矿卡当前任务
        self.truck_current_task = {}
        # 调度开始时间
        self.start_time = datetime.now()
        # # 卡车完成装载及卸载时间
        # self.cur_truck_ava_time = np.zeros(self.dynamic_truck_num)
        # self.sim_truck_ava_time = np.zeros(self.dynamic_truck_num)
        # 矿卡有效载重
        self.payload = np.zeros(self.dynamic_truck_num)
        # 矿卡当前行程(第一列为出发地序号, 第二列为目的地序号)
        self.truck_current_trip = np.full((self.dynamic_truck_num, 2), -1)

    # def period_map_para_load(self):
    #     # 关系映射
    #     self.excavator_uuid_to_index_dict = device_map.excavator_uuid_to_index_dict
    #     self.dump_uuid_to_index_dict = device_map.dump_uuid_to_index_dict
    #     self.excavator_index_to_uuid_dict = device_map.excavator_index_to_uuid_dict
    #     self.dump_index_to_uuid_dict = device_map.dump_index_to_uuid_dict
    #
    #     self.dump_uuid_to_unload_area_uuid_dict = device_map.dump_uuid_to_unload_area_uuid_dict
    #     self.excavator_uuid_to_load_area_uuid_dict = device_map.excavator_uuid_to_load_area_uuid_dict
    #     self.excavator_index_to_load_area_index_dict = device_map.excavator_index_to_load_area_index_dict
    #     self.dump_index_to_unload_area_index_dict = device_map.dump_index_to_unload_area_index_dict
    #
    #     self.truck_uuid_to_index_dict = device_map.truck_uuid_to_index_dict
    #     self.truck_index_to_uuid_dict = device_map.truck_index_to_uuid_dict
    
    # def period_walk_para_load(self):
    #
    #     self.walk_time_to_excavator = walk_manage.walk_time_to_excavator
    #     self.walk_time_to_dump = walk_manage.walk_time_to_dump
    #     self.walk_time_park_to_excavator = walk_manage.walk_time_park_to_excavator
    #     self.walk_time_to_load_area = walk_manage.walk_time_to_load_area
    #     self.walk_time_to_unload_area = walk_manage.walk_time_to_unload_area


    def get_truck_current_trip(self):
        return self.truck_current_trip

    def get_truck_current_task(self):
        return self.truck_current_task

    def get_truck_num(self):
        return self.dynamic_truck_num

    def get_truck_reach_dump(self):
        return self.cur_truck_reach_dump

    def get_truck_reach_excavator(self):
        return self.cur_truck_reach_excavator

    def get_dynamic_truck_set(self):
        return self.dynamic_truck_set

    def get_realative_last_load_time(self):
        return self.relative_last_load_time


    def get_realative_last_unload_time(self):
        return self.relative_unlast_load_time

    def get_payload(self):
        return self.payload

    # 更新矿卡当前任务
    def update_truck_current_task(self):
        self.truck_current_task = {}
        device_name_set = redis2.keys()

        # try:
        for item in device_name_set:
            try:
                item = item.decode(encoding='utf-8')
                json_value = json.loads(redis2.get(item))
                device_type = json_value.get('type')
                if device_type == 1:
                    if truck_name_to_uuid_dict[item] in self.dynamic_truck_set:
                        currentTask = json_value.get('currentTask')
                        self.truck_current_task[truck_name_to_uuid_dict[item]] = currentTask
            except Exception as es:
                logger.error("读取矿卡任务异常-reids读取异常")
                logger.error(es)

        # except Exception as es:
        #     logger.error("读取矿卡任务异常-reids读取异常")
        #     logger.error(es)

        logger.info("矿卡当前任务：")
        logger.info(self.truck_current_task)

    # 更新矿卡实际容量
    def update_truck_payload(self):
        try:
            self.payload = np.zeros(self.dynamic_truck_num)
            for truck_id in self.dynamic_truck_set:
                trcuk_index = self.truck_uuid_to_index_dict[truck_id]
                truck_spec = session_mysql.query(Equipment).filter_by(id=truck_id).first().equipment_spec
                # truck_spec = query.equipment_spec
                self.payload[trcuk_index] = session_mysql.query(EquipmentSpec).filter_by(id=truck_spec).first().capacity
        except Exception as es:
            logger.error("读取矿卡有效载重异常-矿卡型号信息缺失")
            logger.error(es)

    # 更新矿卡最后装载/卸载时间
    def update_truck_last_leave_time(self):
        self.last_load_time = {}
        self.last_unload_time = {}

        self.relative_last_load_time = {}
        self.relative_last_unload_time = {}

        try:

            for item in self.dynamic_truck_set:
                json_value = json.loads(redis2.get(truck_uuid_to_name_dict[item]))
                device_type = json_value.get('type')
                # 判断是否为矿卡
                if device_type == 1:
                    task = self.truck_current_task[item]
                    if task in heavy_task_set:
                        last_load_time_tmp = json_value.get('lastLoadTime')
                        if last_load_time_tmp is not None:
                            self.last_load_time[item] = datetime.strptime(last_load_time_tmp, \
                                                                          "%b %d, %Y %I:%M:%S %p")
                        else:
                            self.last_load_time[item] = datetime.now()
                        self.relative_last_load_time[item] = float((self.last_load_time[item] - self.start_time) /
                                                                   timedelta(hours=0, minutes=1, seconds=0))
                        # print("相对last_load_time", self.relative_last_load_time[item])
                        logger.info("相对last_load_time")
                        logger.info(self.relative_last_load_time[item])
                    if task in empty_task_set:
                        last_unload_time_tmp = json_value.get('lastUnloadTime')
                        if last_unload_time_tmp is not None:
                            self.last_unload_time[item] = datetime.strptime(last_unload_time_tmp, \
                                                                            "%b %d, %Y %I:%M:%S %p")
                        else:
                            self.last_unload_time[item] = datetime.now()
                            json_value['lastUnloadTime'] = datetime.now().strftime('%b %d, %Y %#I:%#M:%#S %p')
                            redis2.set(truck_uuid_to_name_dict[item], str(json.dumps(json_value)))
                            logger.info('lastUnlaodTime is None')
                        self.relative_last_unload_time[item] = float((self.last_unload_time[item] - self.start_time) /
                                                                     timedelta(hours=0, minutes=1, seconds=0))
                        # print("相对last_unload_time", self.relative_last_unload_time[item])
                        logger.info("相对last_unload_time")
                        logger.info(self.relative_last_unload_time[item])
                    elif task == -2:
                        self.last_unload_time[item] = datetime.now()
                        json_value['lastUnloadTime'] = datetime.now().strftime('%b %d, %Y %#I:%#M:%#S %p')
                        redis2.set(truck_uuid_to_name_dict[item], str(json.dumps(json_value)))
        except Exception as es:
            logger.error("读取矿卡可用时间异常-redis读取异常")
            logger.error(es)

    # 更新矿卡行程
    def update_truck_trip(self):

        walk_time_to_load_area = walk_manage.get_walk_time_to_load_area()
        walk_time_to_unload_area = walk_manage.get_walk_time_to_unload_area()

        # 初始化矿卡行程, -1代表备停区
        self.truck_current_trip = np.full((self.dynamic_truck_num, 2), -1)
        for i in range(self.dynamic_truck_num):
            try:
                session_mysql.commit()
                truck_id = self.truck_index_to_uuid_dict[i]
                task = self.truck_current_task[self.truck_index_to_uuid_dict[i]]
                # print("truck_task:", truck_id, task)
                item = session_mysql.query(EquipmentPair).filter_by(truck_id=truck_id, isdeleted=0).first()
                if task in empty_task_set + heavy_task_set and item is None:
                    raise Exception(f'矿卡 {truck_id} 配对关系异常')
            except Exception as es:
                logger.warning(es)

            try:
                # 若矿卡状态为空运
                if task in empty_task_set:
                    last_unload_time = self.relative_last_unload_time[self.truck_index_to_uuid_dict[i]]
                    # 开始区域id
                    start_area_id = self.dump_uuid_to_unload_area_uuid_dict[item.dump_id]
                    # 开始区域序号
                    start_area_index = unload_area_uuid_to_index_dict[start_area_id]
                    end_area_id = self.excavator_uuid_to_load_area_uuid_dict[item.exactor_id]
                    end_area_index = load_area_uuid_to_index_dict[end_area_id]
                    self.truck_current_trip[i] = [self.dump_uuid_to_index_dict[item.dump_id],
                                                  self.excavator_uuid_to_index_dict[item.exactor_id]]
                    # if truck_uuid_to_name_dict[self.truck_index_to_uuid_dict[i]] in tmp_set:
                    #     print("here")
                    #     self.cur_truck_reach_excavator[i] = last_unload_time + 10 * self.walk_time_to_load_area[start_area_index][
                    #         end_area_index]
                    # else:
                    self.cur_truck_reach_excavator[i] = last_unload_time + walk_time_to_load_area[start_area_index][
                        end_area_index]
                # 若矿卡状态为重载
                elif task in heavy_task_set:
                    # print("读取重载行程")
                    # print(item.exactor_id, item.dump_id)
                    last_load_time = self.relative_last_load_time[self.truck_index_to_uuid_dict[i]]
                    # 开始区域id
                    start_area_id = self.excavator_uuid_to_load_area_uuid_dict[item.exactor_id]
                    # 开始区域序号
                    start_area_index = load_area_uuid_to_index_dict[start_area_id]
                    # 结束区域id
                    end_area_id = self.dump_uuid_to_unload_area_uuid_dict[item.dump_id]
                    # 结束区域序号
                    end_area_index = unload_area_uuid_to_index_dict[end_area_id]
                    self.truck_current_trip[i] = [self.excavator_uuid_to_index_dict[item.exactor_id],
                                                  self.dump_uuid_to_index_dict[item.dump_id]]
                    self.cur_truck_reach_dump[i] = last_load_time + walk_time_to_unload_area[end_area_index][start_area_index]
                # 其他状态，矿卡状态为-2，equipment_pair表不存在该矿卡
                else:
                    pass
            except Exception as es:
                logger.error("矿卡行程读取异常")
                logger.error(es)

        self.truck_current_trip.flatten()
        # print("当前矿卡行程：")
        # print(self.truck_current_trip)

    def period_update(self):

        print("Truck update!")

        # # 更新行走队形
        # self.walker.update_walk_time()

        # 装载周期参数
        self.period_map_para_load()

        self.period_walk_para_load()

        # 更新全部矿卡设备集合
        truck_set = set(update_total_truck())

        # 更新固定派车矿卡集合
        fixed_truck_set = set(update_fixdisp_truck())

        # 更新动态派车矿卡集合
        self.dynamic_truck_set = truck_set.difference(fixed_truck_set)

        # 更新矿卡数量
        self.dynamic_truck_num = len(self.dynamic_truck_set)

        # 更新卡车当前任务
        self.update_truck_current_task()

        # 更新有效载重
        self.update_truck_payload()

        # 更新卡车最后一次装载/卸载时间
        self.update_truck_last_leave_time()

        # 更新卡车当前行程
        self.update_truck_trip()


# 调度类
class Dispatcher(WalkManage):
    def __init__(self):
        # object fields
        self.dump = DumpInfo()
        self.excavator = ExcavatorInfo()
        self.truck = TruckInfo()
        # self.walker = WalkManage()

        # 模拟挖机/卸载设备产量(防止调度修改真实产量)
        self.sim_dump_real_mass = np.zeros(self.dump.get_dump_num())
        self.sim_excavator_real_mass = np.zeros(self.excavator.get_excavator_num())
        # 真实设备可用时间
        self.cur_truck_reach_dump = np.zeros(self.truck.get_truck_num())
        self.cur_truck_reach_excavator = np.zeros(self.truck.get_truck_num())
        self.cur_excavator_ava_time = np.zeros(self.excavator.get_excavator_num())
        self.cur_dump_ava_time = np.zeros(self.dump.get_dump_num())
        # 卡车完成装载及卸载时间(矿卡可用时间)
        self.cur_truck_ava_time = np.zeros(self.truck.get_truck_num())
        # 模拟矿卡可用时间
        self.sim_truck_ava_time = np.zeros(self.truck.get_truck_num())
        # 模拟各设备可用时间(防止调度修改真实产量)
        self.sim_excavator_ava_time = np.zeros(self.excavator.get_excavator_num())
        self.sim_dump_ava_time = np.zeros(self.dump.get_dump_num())
        # 挖机/卸载设备预计产量(包含正在驶往挖机/卸载设备那部分矿卡的载重)
        self.pre_dump_real_mass = np.zeros(self.dump.get_dump_num())
        self.pre_excavator_real_mass = np.zeros(self.excavator.get_excavator_num())

        # 维护一个矿卡调度表
        self.Seq = [[] for _ in range(self.truck.get_truck_num())]
        # 调度开始时间
        self.start_time = datetime.now()
        # self.relative_now_time = datetime.now() - self.start_time

        # 下面是交通流调度部分
        # 驶往挖机的实际车流
        self.actual_goto_excavator_traffic_flow = np.zeros((self.dump.get_dump_num(), self.excavator.get_excavator_num()))
        # 驶往卸载设备的实际车流
        self.actual_goto_dump_traffic_flow = np.zeros((self.dump.get_dump_num(), self.excavator.get_excavator_num()))

        # 驶往挖机的实际车次
        self.goto_dump_truck_num = np.zeros((self.dump.get_dump_num(), self.excavator.get_excavator_num()))
        # 驶往卸载设备的实际车次
        self.goto_excavator_truck_num = np.zeros((self.dump.get_dump_num(), self.excavator.get_excavator_num()))

        # 驶往挖机的理想车流
        self.opt_goto_dump_traffic_flow = np.zeros((self.dump.get_dump_num(), self.excavator.get_excavator_num()))
        # 驶往卸载设备的实际车流
        self.opt_goto_excavator_traffic_flow = np.zeros((self.dump.get_dump_num(), self.excavator.get_excavator_num()))

    # 更新矿卡预计抵达目的地时间
    def update_truck_reach_time(self):
        try:
            dynamic_excavator_num = self.excavator.get_excavator_num()
            dumps = self.dump.get_dump_num()
            trucks = self.truck.get_truck_num()

            truck_current_task = self.truck.get_truck_current_task()

            truck_current_trip = self.truck.get_truck_current_trip()

            cur_truck_reach_excavator = self.truck.get_truck_reach_excavator()

            cur_truck_reach_dump = self.truck.get_truck_reach_dump()

            excavator_ava_ls = [[] for _ in range(dynamic_excavator_num)]
            dump_ava_ls = [[] for _ in range(dumps)]
            for i in range(trucks):
                task = truck_current_task[self.truck_index_to_uuid_dict[i]]
                end_area_index = truck_current_trip[i][1]
                if task in [0, 1]:
                    reach_time = cur_truck_reach_excavator[i]
                    excavator_ava_ls[end_area_index].append([reach_time, i, end_area_index])
                elif task in [3, 4]:
                    reach_time = cur_truck_reach_dump[i]
                    dump_ava_ls[end_area_index].append([reach_time, i, end_area_index])
                elif task == -2:
                    self.cur_truck_ava_time[i] = (datetime.now() - self.start_time) / timedelta(hours=0, minutes=1,
                                                                                                seconds=0)

            print(self.truck_index_to_uuid_dict)
            print(excavator_ava_ls)
            print(dump_ava_ls)
        except Exception as es:
            logger.error("矿卡预计抵达时间计算异常")
            logger.error(es)

        return excavator_ava_ls, dump_ava_ls

    # 更新挖机预计可用时间
    def update_excavator_ava_time(self, excavator_ava_ls):

        # 初始化挖机可用时间
        self.cur_excavator_ava_time = np.full(dynamic_excavator_num,
                                           (datetime.now() - self.start_time) / timedelta(hours=0, minutes=1,
                                                                                          seconds=0))

        loading_time = self.excavator.get_loading_time()

        loading_task_time = self.excavator.get_loading_task_time()

        try:

            now = float((datetime.now() - self.start_time) / timedelta(hours=0, minutes=1, seconds=0))

            for reach_ls in excavator_ava_ls:
                if len(reach_ls) != 0:
                    reach_ls = np.array(reach_ls)
                    tmp = reach_ls[np.lexsort(reach_ls[:, ::-1].T)]
                    for i in range(len(tmp)):
                        excavator_index = int(tmp[i][2])
                        self.cur_excavator_ava_time[excavator_index] = max(tmp[i][0],
                                                                     self.cur_excavator_ava_time[excavator_index]) + \
                                                                 loading_task_time[excavator_index]
                        self.cur_truck_ava_time[int(tmp[i][1])] = self.cur_excavator_ava_time[excavator_index]

                        # # 若挖机可用时间严重偏离，进行修正
                        # if abs(self.cur_excavator_ava_time[excavator_index] - now) > 60:
                        #     self.cur_truck_ava_time[int(tmp[i][1])] = now
                        # if abs(self.cur_excavator_ava_time[excavator_index] - now) > 60:
                        #     self.cur_excavator_ava_time[excavator_index] = now
        except Exception as es:
            logger.error("挖机可用时间计算异常")
            logger.error(es)

    # 更新卸载设备预计可用时间
    def update_dump_ava_time(self, dump_ava_ls):

        dynamic_dump_num = self.dump.get_dump_num()

        # 初始化卸载设备可用时间
        self.cur_dump_ava_time = np.full(dynamic_dump_num,
                                           (datetime.now() - self.start_time) / timedelta(hours=0, minutes=1,
                                                                                          seconds=0))

        unloading_time = self.dump.get_unloading_time()

        unloading_task_time = self.dump.get_unloading_task_time()

        try:

            now = float((datetime.now() - self.start_time) / timedelta(hours=0, minutes=1, seconds=0))

            for reach_ls in dump_ava_ls:
                if len(reach_ls) != 0:
                    reach_ls = np.array(reach_ls)
                    tmp = reach_ls[np.lexsort(reach_ls[:, ::-1].T)]
                    for i in range(len(tmp)):
                        dump_index = int(tmp[i][2])
                        self.cur_dump_ava_time[dump_index] = max(tmp[i][0], self.cur_dump_ava_time[dump_index]) + \
                                                             unloading_task_time[dump_index]
                        self.cur_truck_ava_time[int(tmp[i][1])] = self.cur_dump_ava_time[dump_index]

                        # # 若卸载设备可用时间严重偏离，进行修正
                        # if abs(self.cur_dump_ava_time[dump_index] - now) > 60:
                        #     self.cur_dump_ava_time[dump_index] = now
                        # if abs(self.cur_truck_ava_time[int(tmp[i][1])] - now) > 60:
                        #     self.cur_truck_ava_time[int(tmp[i][1])] = now
        except Exception as es:
            logger.error("卸载设备可用时间计算异常")
            logger.error(es)

    # 更新实际交通流
    def update_actual_traffic_flow(self):

        loading_time = self.excavator.get_loading_time()

        unloading_time = self.dump.get_unloading_time()

        loading_task_time = self.excavator.get_loading_task_time()

        unloading_task_time = self.dump.get_unloading_task_time()

        truck_current_task = self.truck.get_truck_current_task()
        truck_current_trip = self.truck.get_truck_current_trip()
        payload = self.truck.get_payload()

        dynamic_dump_num = self.dump.get_dump_num()
        dynamic_excavator_num = self.excavator.get_excavator_num()

        # for item in session_mysql.query(EquipmentPair).filter(EquipmentPair.createtime >= self.start_time).all():
        #     dump_index = self.dump_uuid_to_index_dict[item.dump_id]
        #     excavator_index = self.excavator_uuid_to_index_dict[item.exactor_id]
        #     task = truck_current_task[item.truck_id]
        #     if task in heavy_task_set:
        #         self.goto_dump_truck_num[dump_index][excavator_index] = \
        #             self.goto_dump_truck_num[dump_index][excavator_index] + 1
        #         self.actual_goto_dump_traffic_flow[dump_index][excavator_index] = \
        #             self.actual_goto_dump_traffic_flow[dump_index][excavator_index] + float(
        #                 payload[self.truck_uuid_to_index_dict[item.truck_id]])
        #     if task in empty_task_set or task == -2:
        #         self.goto_excavator_truck_num[dump_index][excavator_index] = \
        #             self.goto_excavator_truck_num[dump_index][excavator_index] + 1
        #         print(item.truck_id)
        #         self.actual_goto_excavator_traffic_flow[dump_index][excavator_index] = \
        #             self.actual_goto_excavator_traffic_flow[dump_index][excavator_index] + float(
        #                 payload[self.truck_uuid_to_index_dict[item.truck_id]])

        self.goto_dump_truck_num = np.zeros((dynamic_excavator_num, dynamic_dump_num))
        self.actual_goto_dump_traffic_flow = np.zeros((dynamic_excavator_num, dynamic_dump_num))
        self.goto_excavator_truck_num = np.zeros((dynamic_dump_num, dynamic_excavator_num))
        self.actual_goto_excavator_traffic_flow = np.zeros((dynamic_dump_num, dynamic_excavator_num))

        try:
            for i in range(dynamic_truck_num):
                task = truck_current_task[self.truck_index_to_uuid_dict[i]]
                end_area_index = truck_current_trip[i][1]
                start_area_index = truck_current_trip[i][0]

                # 若矿卡正常行驶，需要将该部分载重计入实时产量
                if task in heavy_task_set:
                    self.goto_dump_truck_num[end_area_index][start_area_index] = \
                        self.goto_dump_truck_num[end_area_index][start_area_index] + 1
                    self.actual_goto_dump_traffic_flow[end_area_index][start_area_index] = \
                        self.actual_goto_dump_traffic_flow[end_area_index][start_area_index] + float(
                            payload[i])
                if task in empty_task_set:
                    self.goto_excavator_truck_num[start_area_index][end_area_index] = \
                        self.goto_excavator_truck_num[start_area_index][end_area_index] + 1
                    self.actual_goto_excavator_traffic_flow[start_area_index][end_area_index] = \
                        self.actual_goto_excavator_traffic_flow[start_area_index][end_area_index] + float(
                            payload[i])

            # print(np.expand_dims(unloading_time,axis=0).repeat(dynamic_excavator_num, axis=0))

            self.actual_goto_dump_traffic_flow = self.actual_goto_dump_traffic_flow / \
                             (self.distance_to_dump.reshape(dynamic_excavator_num, dynamic_dump_num) / (1000 * empty_speed) + \
                              np.expand_dims(unloading_task_time,axis=0).repeat(dynamic_excavator_num, axis=0))

        except Exception as es:
            logger.error("更新不及时")
            logger.error(es)

        # print("驶往卸点实际载重")
        # print(self.actual_goto_dump_traffic_flow)
        # print("卸点路段行驶时间(h)")
        # print((self.distance_to_dump.reshape(dynamic_excavator_num, dynamic_dump_num) / (1000 * empty_speed)))
        # print("驶往卸点实际车流")
        # print(self.actual_goto_dump_traffic_flow)

        logger.info("驶往卸点实际载重")
        logger.info(self.actual_goto_dump_traffic_flow)
        logger.info("卸点路段行驶时间(h)")
        logger.info((self.distance_to_dump.reshape(dynamic_excavator_num, dynamic_dump_num) / (1000 * empty_speed)))
        logger.info("驶往卸点实际车流")
        logger.info(self.actual_goto_dump_traffic_flow)

        self.actual_goto_excavator_traffic_flow = self.actual_goto_excavator_traffic_flow / \
                  (self.distance_to_excavator.reshape(dynamic_excavator_num, dynamic_dump_num) / (1000 * heavy_speed) + \
                   np.expand_dims(loading_task_time,axis=0).repeat(dynamic_dump_num, axis=0))

        # print("驶往挖机实际载重")
        # print(self.actual_goto_excavator_traffic_flow)
        # print("挖机路段行驶时间(h)")
        # print((self.distance_to_excavator.reshape(dynamic_excavator_num, dynamic_dump_num) / (1000 * heavy_speed)))
        # print("驶往挖机实际车流")
        # print(self.actual_goto_excavator_traffic_flow)

        logger.info("驶往挖机实际载重")
        logger.info(self.actual_goto_excavator_traffic_flow)
        logger.info("挖机路段行驶时间(h)")
        logger.info((self.distance_to_excavator.reshape(dynamic_excavator_num, dynamic_dump_num) / (1000 * heavy_speed)))
        logger.info("驶往挖机实际车流")
        logger.info(self.actual_goto_excavator_traffic_flow)

    # 更新卸载设备预计产量
    def update_pre_unload_throughout(self):

        truck_current_task = self.truck.get_truck_current_task()
        payload = self.truck.get_payload()

        try:
            self.pre_dump_real_mass = copy.deepcopy(self.dump.get_dump_actual_mass())
            for i in range(self.truck.get_truck_num()):
                # task = self.truck_current_stage[i][0]
                task = truck_current_task[self.truck_index_to_uuid_dict[i]]
                end_area_index = self.truck.get_truck_current_trip()[i][1]
                # 若矿卡正常行驶，需要将该部分载重计入实时产量
                if task in heavy_task_set:
                    self.pre_dump_real_mass[end_area_index] = self.pre_dump_real_mass[end_area_index] + payload[i]
                else:
                    pass
        except Exception as es:
            logger.error("卸载设备预计装载量计算异常")
            logger.error(es)

    # 更新挖机预计产量
    def update_pre_load_throughout(self):

        truck_current_task = self.truck.get_truck_current_task()
        payload = self.truck.get_payload()

        try:
            self.pre_excavator_real_mass = copy.deepcopy(self.excavator.get_excavator_actual_mass())
            for i in range(self.truck.get_truck_num()):
                # task = self.truck_current_stage[i][0]
                task = truck_current_task[self.truck_index_to_uuid_dict[i]]
                end_area_index = self.truck.get_truck_current_trip()[i][1]
                # 若矿卡正常行驶，需要将该部分载重计入实时产量
                if task in empty_task_set:
                    self.pre_excavator_real_mass[end_area_index] = self.pre_excavator_real_mass[end_area_index] + \
                                                                   payload[i]
                else:
                    pass
        except Exception as es:
            logger.error("挖机/卸载设备预计装载量计算异常")
            logger.error(es)

    def period_update(self):

        logger.info("#####################################周期更新开始#####################################")

        # 装载周期参数
        self.period_map_para_load()

        self.period_walk_para_load()

        # 更新卸载设备对象
        self.dump.period_update()

        # 更新挖机对象
        self.excavator.period_update()

        # 更新矿卡对象
        self.truck.period_update()

        # # 更新距离参量
        # self.walker.update_walk_time()

        # # 更新设备距离（不同于工作区距离）
        # self.update_walk_time()

        # 更新实时车流
        self.update_actual_traffic_flow()

        # 计算理想车流
        self.opt_goto_dump_traffic_flow, self.opt_goto_excavator_traffic_flow = traffic_flow_plan()

        # 矿卡抵达时间
        excavator_reach_list, dump_reach_list = self.update_truck_reach_time()

        # 挖机可用时间
        self.update_excavator_ava_time(excavator_reach_list)

        # 卸载设备可用时间
        self.update_dump_ava_time(dump_reach_list)

        # 挖机预计装载量
        self.update_pre_load_throughout()

        # 卸载设备预计卸载量
        self.update_pre_unload_throughout()

    def sim_para_reset(self):

        # 设备可用时间重置
        self.sim_truck_ava_time = copy.deepcopy(self.cur_truck_ava_time)
        self.sim_excavator_ava_time = copy.deepcopy(self.cur_excavator_ava_time)
        self.sim_dump_ava_time = copy.deepcopy(self.cur_dump_ava_time)

        # 挖机\卸载设备产量重置
        self.sim_dump_real_mass = copy.deepcopy(self.pre_dump_real_mass)
        self.sim_excavator_real_mass = copy.deepcopy(self.pre_excavator_real_mass)

    def truck_schedule(self, truck_id):

        # 矿卡对应序号
        truck_index = self.truck_uuid_to_index_dict[truck_id]
        # 矿卡行程
        trip = self.truck.get_truck_current_trip()[truck_index]
        # 矿卡当前任务
        task = self.truck.get_truck_current_task()[self.truck_index_to_uuid_dict[truck_index]]
        # 挖机目标产量
        excavator_target_mass = self.excavator.get_excavator_target_mass()
        # 挖机装载时间
        loading_time = self.excavator.get_loading_time()
        # 卸载设备目标产量
        dump_target_mass = self.dump.get_dump_target_mass()
        # 卸载设备卸载时间
        unloading_time = self.dump.get_unloading_time()
        # 路网信息
        walk_time_park_to_excavator = walk_manage.get_walk_time_park_to_excavator()
        walk_time_to_dump = walk_manage.get_walk_time_to_dump()
        walk_time_to_excavator = walk_manage.get_walk_time_to_excavator()

        # 出入场时间
        loading_task_time = self.excavator.get_loading_task_time()
        unloading_task_time = self.dump.get_unloading_task_time()

        dynamic_dump_num = self.dump.get_dump_num()
        dynamic_excavator_num = self.excavator.get_excavator_num()

        now = float((datetime.now() - self.start_time) / timedelta(hours=0, minutes=1, seconds=0))

        # print()
        # print("调度矿卡：", truck_id, truck_uuid_to_name_dict[truck_id])
        logger.info(" ")
        logger.info(f'调度矿卡 {truck_id}  {truck_uuid_to_name_dict[truck_id]}')

        target = 0

        if task == -2:
            try:
                logger.info("矿卡状态：矿卡启动或故障恢复")
                logger.info("矿卡行程：无")
                logger.info(f'涉及挖机：{list(self.excavator_uuid_to_index_dict.keys())}')
                logger.info(f'挖机饱和度：{(1 - self.sim_excavator_real_mass / excavator_target_mass)}')
                logger.info(
                    f'行程时间：{(np.maximum(self.sim_excavator_ava_time, now + walk_time_park_to_excavator[0, :]) + loading_time - now)}')
                logger.info(f'行驶时间：{walk_time_park_to_excavator[0, :] + loading_time}')
            except Exception as es:
                logger.error(f'矿卡{truck_id}状态不匹配')
                logger.error(es)

            target = np.argmax(10 * (1 - self.sim_excavator_real_mass / excavator_target_mass) /
                               (np.maximum(self.sim_excavator_ava_time,
                                           now + walk_time_park_to_excavator[0, :]) + loading_task_time
                                - now))

            # target = np.argmin((walk_time_park_to_excavator[0, :] + loading_time))

            logger.info(f'目的地：{self.excavator_index_to_uuid_dict[target]}')
            # print("目的地: ", self.excavator_index_to_uuid_dict[target])

        if task in [0, 1, 2]:
            try:
                logger.info("矿卡状态：矿卡空载")
                # logger.info(f'矿卡行程：{self.dump_index_to_uuid_dict[trip[0]]}-{self.excavator_index_to_uuid_dict[trip[1]]}')
                logger.info(f'涉及卸载设备：{list(self.dump_uuid_to_index_dict.keys())}')
                # logger.info(f'卸载设备饱和度：{(1 - self.sim_dump_real_mass / dump_target_mass)}')
                # logger.info(
                #     f'行程时间：{(np.maximum(self.sim_dump_ava_time, self.sim_truck_ava_time[truck_index] + walk_time_to_dump[:, trip[1]]) + unloading_time - self.sim_truck_ava_time[truck_index])}')
                # logger.info(f'行驶时间：{walk_time_to_dump[:, trip[1]] + unloading_time}')
            except Exception as es:
                logger.error(f'矿卡{truck_id}状态不匹配')
                logger.error(es)

            # # 卡车空载，计算下一次卸载设备
            # target = np.argmax(10 * (1 - self.sim_dump_real_mass / dump_target_mass) /
            #                    (np.maximum(self.sim_dump_ava_time,
            #                                # self.sim_truck_reach_excavator[truck_index] + self.loading_time[trip[1]]
            #                                self.sim_truck_ava_time[truck_index]
            #                                + walk_time_to_dump[:, trip[1]]) + unloading_time
            #                     - self.sim_truck_ava_time[truck_index]))

            try:
                assert np.array(self.actual_goto_dump_traffic_flow).shape == (dynamic_excavator_num, dynamic_dump_num)
                assert np.array(self.opt_goto_dump_traffic_flow).shape == (dynamic_excavator_num, dynamic_dump_num)
            except Exception as es:
                logger.warning(es)
                self.actual_goto_dump_traffic_flow = \
                    np.array(self.actual_goto_dump_traffic_flow).reshape((dynamic_excavator_num, dynamic_dump_num))
                self.opt_goto_dump_traffic_flow = \
                    np.array(self.opt_goto_dump_traffic_flow).reshape((dynamic_excavator_num, dynamic_dump_num))

            self.actual_goto_dump_traffic_flow = np.array(self.actual_goto_dump_traffic_flow)
            self.opt_goto_dump_traffic_flow = np.array(self.opt_goto_dump_traffic_flow)

            logger.info('挖机id:')
            logger.info(self.excavator_uuid_to_index_dict)
            logger.info('卸点id:')
            logger.info(self.dump_uuid_to_index_dict)
            logger.info(f'卸载点实际车流:')
            logger.info(self.actual_goto_dump_traffic_flow)
            logger.info(f'卸载点理想车流:')
            logger.info(self.opt_goto_dump_traffic_flow)

            logger.info("卸载点实际车流")
            logger.info(self.actual_goto_dump_traffic_flow[int(trip[1]), :])
            logger.info("卸载点理想车流")
            logger.info(self.opt_goto_dump_traffic_flow[int(trip[1]), :])

            target = np.argmin(
                (self.actual_goto_dump_traffic_flow[int(trip[1]), :] + 0.001) / (self.opt_goto_dump_traffic_flow[int(trip[1]), :] + 0.001))

            logger.info("车流比:")
            logger.info((self.actual_goto_dump_traffic_flow[int(trip[1]), :] + 0.001) / (self.opt_goto_dump_traffic_flow[int(trip[1]), :] + 0.001))

            logger.info(f'目的地：{self.dump_index_to_uuid_dict[target]}')

        elif task in [3, 4, 5]:

            try:

                logger.info("矿卡状态：矿卡重载")
                # logger.info(f'矿卡行程：{self.excavator_index_to_uuid_dict[trip[0]]}-{self.dump_index_to_uuid_dict[trip[1]]}')
                logger.info(f'涉及挖机设备：{list(self.excavator_uuid_to_index_dict.keys())}')
                # logger.info(f'卸载设备饱和度：{(1 - self.sim_excavator_real_mass / excavator_target_mass)}')
                # logger.info(
                #     f'行程时间：{(np.maximum(self.sim_excavator_ava_time, self.sim_truck_ava_time[truck_index] + walk_time_to_excavator[trip[1], :]) + loading_time - self.sim_truck_ava_time[truck_index])}')
                # logger.info(f'行驶时间：{walk_time_to_excavator[trip[1], :] + loading_time}')
            except Exception as es:
                logger.error(f'矿卡{truck_id}状态不匹配')
                logger.error(es)

            # # 卡车重载，计算下一次装载点
            # target = np.argmax(10 * (1 - self.sim_excavator_real_mass / excavator_target_mass) /
            #                    (np.maximum(self.sim_excavator_ava_time,
            #                                self.sim_truck_ava_time[truck_index]
            #                                + walk_time_to_excavator[trip[1], :]) + loading_time
            #                     - self.sim_truck_ava_time[truck_index]))

            try:
                assert np.array(self.actual_goto_excavator_traffic_flow).shape == (dynamic_excavator_num, dynamic_dump_num)
                assert np.array(self.opt_goto_excavator_traffic_flow).shape == (dynamic_excavator_num, dynamic_dump_num)
            except Exception as es:
                logger.warning(es)
                self.actual_goto_excavator_traffic_flow = \
                    np.array(self.actual_goto_excavator_traffic_flow).reshape((dynamic_dump_num, dynamic_excavator_num))
                self.opt_goto_excavator_traffic_flow = \
                    np.array(self.opt_goto_excavator_traffic_flow).reshape((dynamic_dump_num, dynamic_excavator_num))

            # 不知道为什么，偶尔变成了list
            self.actual_goto_excavator_traffic_flow = np.array(self.actual_goto_excavator_traffic_flow)
            self.opt_goto_excavator_traffic_flow = np.array(self.opt_goto_excavator_traffic_flow)

            logger.info('挖机id:')
            logger.info(self.excavator_uuid_to_index_dict)
            logger.info('卸点id:')
            logger.info(self.dump_uuid_to_index_dict)
            logger.info(f'挖机实际车流:')
            logger.info(self.actual_goto_excavator_traffic_flow)
            logger.info(f'挖机理想车流:')
            logger.info(self.opt_goto_excavator_traffic_flow)

            logger.info("挖机实际车流")
            logger.info(self.actual_goto_excavator_traffic_flow[trip[1], :])
            logger.info("挖机理想车流")
            logger.info(self.opt_goto_excavator_traffic_flow[trip[1], :])

            target = np.argmin(
                (self.actual_goto_excavator_traffic_flow[trip[1], :] + 0.001) / (self.opt_goto_excavator_traffic_flow[trip[1], :] + 0.001))

            logger.info("车流比:")
            logger.info((self.actual_goto_excavator_traffic_flow[trip[1], :] + 0.001) / (self.opt_goto_excavator_traffic_flow[trip[1], :] + 0.001))

            logger.info(f'目的地：{self.excavator_index_to_uuid_dict[target]}')

        return target

    def schedule_construct(self):

        try:

            # 读取所需信息
            trucks = self.truck.get_truck_num()
            truck_current_trip = self.truck.get_truck_current_trip()
            truck_current_task = self.truck.get_truck_current_task()
            payload = self.truck.get_payload()
            unloading_time = self.dump.get_unloading_time()
            loading_time = self.excavator.get_loading_time()

            # 出入场时间
            loading_task_time = self.excavator.get_loading_task_time()
            unloading_task_time = self.dump.get_unloading_task_time()

            walk_time_to_unload_area = walk_manage.get_walk_time_to_unload_area()
            walk_time_to_load_area = walk_manage.get_walk_time_to_load_area()

            # Seq初始化
            Seq = [[truck_current_trip[i][1], -1] for i in range(trucks)]

            # 根据矿卡最早可用时间顺序进行规划
            temp = copy.deepcopy(self.cur_truck_ava_time)

            # 没有启动的矿卡加上一个很大的值，降低其优先级
            for i in range(trucks):
                task = truck_current_task[self.truck_index_to_uuid_dict[i]]
                if task == -2:
                    temp[i] = temp[i] + M

            index = np.argsort(temp.reshape(1, -1))
            index = index.flatten()

            # 对于在线矿卡已经赋予新的派车计划，更新其最早可用时间，及相关设备时间参数
            for truck in index:
                if len(Seq[truck]) > 0:

                    # try:
                    task = truck_current_task[self.truck_index_to_uuid_dict[truck]]

                    # 矿卡结束当前派车计划后的目的地
                    end_eq_index = truck_current_trip[truck][1]

                    # 调用调度函数，得到最优目的地序号
                    target_eq_index = self.truck_schedule(self.truck_index_to_uuid_dict[truck])

                    # 写入Seq序列
                    Seq[truck][1] = target_eq_index

                    # except Exception as es:
                    #     logger.error(f'矿卡 {truck_uuid_to_name_dict[self.truck_index_to_uuid_dict[truck]]} 派车计划计算异常')
                    #     logger.error(es)

                    try:

                        if task in empty_task_set:
                            target_area_index = self.dump_index_to_unload_area_index_dict[target_eq_index]
                            end_area_index = self.excavator_index_to_load_area_index_dict[end_eq_index]
                            # 更新变量，预计产量更新
                            self.sim_dump_real_mass[target_eq_index] = self.sim_dump_real_mass[target_eq_index] + \
                                                                       payload[truck]
                            # 预计卸载设备可用时间更新
                            self.sim_dump_ava_time[target_eq_index] = (
                                    max(
                                        self.sim_dump_ava_time[target_eq_index],
                                        self.sim_truck_ava_time[truck]
                                        + walk_time_to_unload_area[target_area_index][end_area_index],
                                    )
                                    + unloading_task_time[target_eq_index]
                            )
                        elif task in heavy_task_set:
                            target_area_index = self.excavator_index_to_load_area_index_dict[target_eq_index]
                            end_area_index = self.dump_index_to_unload_area_index_dict[end_eq_index]
                            # 更新变量，预计产量更新
                            self.sim_excavator_real_mass[target_eq_index] = self.sim_excavator_real_mass[target_eq_index] + \
                                                                         payload[truck]
                            # 预计装载点可用时间更新
                            self.sim_excavator_ava_time[target_eq_index] = (
                                    max(
                                        self.sim_excavator_ava_time[target_eq_index],
                                        self.sim_truck_ava_time[truck]
                                        + walk_time_to_unload_area[end_area_index][target_area_index],
                                    )
                                    + loading_task_time[target_eq_index]
                            )
                        else:
                            pass
                    except Exception as es:
                        logger.error(f'矿卡 {truck_uuid_to_name_dict[self.truck_index_to_uuid_dict[truck]]} 调度状态更新异常')
                        logger.error(es)

            for i in range(len(Seq)):
                try:

                    record = {"truckId": self.truck_index_to_uuid_dict[i]}
                    task = self.truck.get_truck_current_task()[self.truck_index_to_uuid_dict[i]]
                    if task in empty_task_set:
                        item = session_mysql.query(Dispatch).filter_by(
                            dump_id=self.dump_index_to_uuid_dict[Seq[i][1]], isauto=1, isdeleted=0).first()
                        record["exactorId"] = item.exactor_id
                        record["dumpId"] = item.dump_id
                        record["loadAreaId"] = item.load_area_id
                        record["unloadAreaId"] = item.unload_area_id
                        record["dispatchId"] = item.id
                        record["isdeleted"] = False
                        record["creator"] = item.creator
                        record["createtime"] = item.createtime.strftime('%b %d, %Y %#I:%#M:%#S %p')
                    elif task in heavy_task_set:
                        item = session_mysql.query(Dispatch).filter_by(
                            exactor_id=self.excavator_index_to_uuid_dict[Seq[i][1]], isauto=1, isdeleted=0).first()
                        record["exactorId"] = self.excavator_index_to_uuid_dict[Seq[i][1]]
                        record["dumpId"] = item.dump_id
                        record["loadAreaId"] = item.load_area_id
                        record["unloadAreaId"] = item.unload_area_id
                        record["dispatchId"] = item.id
                        record["isdeleted"] = False
                        record["creator"] = item.creator
                        record["createtime"] = item.createtime.strftime('%b %d, %Y %#I:%#M:%#S %p')
                    elif task == -2:
                        item = session_mysql.query(Dispatch).filter_by(
                            exactor_id=self.excavator_index_to_uuid_dict[Seq[i][1]], isauto=1, isdeleted=0).first()
                        record["exactorId"] = item.exactor_id
                        record["dumpId"] = item.dump_id
                        record["loadAreaId"] = item.load_area_id
                        record["unloadAreaId"] = item.unload_area_id
                        record["dispatchId"] = item.id
                        record["isdeleted"] = False
                        record["creator"] = item.creator
                        record["createtime"] = item.createtime.strftime('%b %d, %Y %#I:%#M:%#S %p')
                    else:
                        pass

                    redis5.set(self.truck_index_to_uuid_dict[i], str(json.dumps(record)))
                except Exception as es:
                    logger.error("调度结果写入异常-redis写入异常")
                    logger.error(f'调度结果:{Seq}')
                    logger.error(es)

            for i in range(trucks):
                print("dispatch_setting:")
                print(redis5.get(self.truck_index_to_uuid_dict[i]))
        except Exception as es:
            logger.error("更新不及时")
            logger.error(es)



        logger.info("#####################################周期更新结束#####################################")

        return Seq



# 下面三个函数保证程序定期执行，不用管他
def process(dispatcher):

    # 更新周期参数
    period_para_update()

    # 清空数据库缓存
    session_mysql.commit()
    session_mysql.flush()

    # 周期更新
    dispatcher.period_update()

    # 参数重置
    dispatcher.sim_para_reset()

    try:

        # 调度计算
        dispatcher.schedule_construct()

    except Exception as es:
        logger.error("更新不及时")
        logger.error(es)


scheduler = sched.scheduler(time.time, time.sleep)


def perform(inc, dispatcher):
    scheduler.enter(inc, 0, perform, (inc, dispatcher))
    process(dispatcher)


def main(inc, dispatcher):
    scheduler.enter(0, 0, perform, (inc, dispatcher))
    scheduler.run()


if __name__ == "__main__":
    logger.info(" ")
    logger.info("调度系统启动")

    dispatcher = Dispatcher()

    main(10, dispatcher)
