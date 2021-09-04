#!E:\Pycharm Projects\Waytous
# -*- coding: utf-8 -*-
# @Time : 2021/8/3 10:41
# @Author : Opfer
# @Site :
# @File : traffic_flow_info.py
# @Software: PyCharm

# import

from path_plan.path_plannner import *
from traffic_flow.traffic_flow_planner import *
from para_config import *
from equipment.excavator import ExcavatorInfo
from equipment.dump import DumpInfo
from equipment.truck import TruckInfo

# 车流规划类
class Traffic_para(WalkManage):
    def __init__(self, num_of_load_area, num_of_unload_area, num_of_excavator, num_of_dump):
        self.num_of_dump = num_of_dump
        self.num_of_excavator = num_of_excavator
        self.num_of_load_area = num_of_load_area
        self.num_of_unload_area = num_of_unload_area

        self.walk_time_to_load_area = np.zeros((num_of_unload_area, num_of_load_area))  # 空载运输路线距离
        self.walk_time_to_unload_area = np.zeros((num_of_load_area, num_of_unload_area))  # 重载运输路线距离

        # self.avg_goto_excavator_weight = np.zeros((num_of_load_area, num_of_unload_area))
        self.avg_goto_excavator_weight = np.full((num_of_unload_area, num_of_load_area), 1)

        # self.avg_goto_dump_weight = np.zeros((num_of_load_area, num_of_unload_area))
        self.avg_goto_dump_weight = np.full((num_of_load_area, num_of_unload_area), 1)
        self.walk_time_to_excavator = np.zeros((num_of_dump, num_of_excavator))  # 逻辑空载运输路线距离
        self.walk_time_to_dump = np.zeros((num_of_excavator, num_of_dump))  # 逻辑重载运输路线距离

        # self.payload = 200  # 有效载重(不同型号矿卡载重不同，这里暂时认为车队是同质的)
        self.truck = TruckInfo()
        self.payload = np.mean(self.truck.get_payload())

        self.empty_speed = sum(self.truck.empty_speed.values()) / self.truck.get_truck_num()  # 空载矿卡平均时速
        self.heavy_speed = sum(self.truck.heavy_speed.values()) / self.truck.get_truck_num() # 重载矿卡平均时速

        self.min_throughout = 1000  # 最小产量约束
        self.truck_total_num = 0

        self.excavator_strength = np.zeros(num_of_excavator)  # 用于保存电铲的工作强度,单位是t/h
        self.dump_strength = np.zeros(num_of_dump)  # 卸载点的工作强度，单位是t/h

        self.path_planner = PathPlanner()
        self.path_planner.walk_cost()

        self.excavator = ExcavatorInfo()
        self.dump = DumpInfo()

        self.excavator_priority_coefficient = np.ones(num_of_excavator)  # 每个电铲的优先级系数
        self.excavator_material_priority = np.ones(num_of_excavator) # 每个电铲的物料优先级系数
        self.grade_loading_array = np.zeros(num_of_excavator)  # 用于保存电铲挖掘矿石的品位
        self.dump_priority_coefficient = np.ones(num_of_dump)  # 每个卸载点的优先级系数
        self.dump_material_priority = np.ones(num_of_excavator)  # 每个卸载点的物料优先级系数
        # 卸载道路的运输系数：卸载道路上，每运输1吨货物需要一辆卡车运行时长,等于（该卸载道路上车辆平均运行时长/卡车平均实际装载量）
        self.goto_unload_area_factor = np.full((num_of_load_area, num_of_unload_area), 10, dtype=float)
        # 装载道路的运输系数，装载道路上，每提供1吨的装载能力需要一辆卡车运行时长,等于（该装载道路上车辆平均运行时长/卡车平均装载能力）
        self.goto_load_area_factor = np.full((num_of_unload_area, num_of_load_area), 10, dtype=float)
        self.goto_dump_factor = np.full((num_of_excavator, num_of_dump), 10, dtype=float) # 逻辑卸载道路的运输系数
        self.goto_excavator_factor = np.full((num_of_dump, num_of_excavator), 10, dtype=float)  # 逻辑装载道路的运输系数
        self.priority_coefficient_goto_dump = np.ones((num_of_excavator, num_of_dump))  # 卸载道路的优先级系数
        self.priority_coefficient_goto_excavator = np.ones((num_of_dump, num_of_excavator))  # 装载道路的优先级系数
        self.grade_lower_dump_array = np.zeros(num_of_dump)  # 卸载点矿石品位下限
        self.grade_upper_dump_array = np.zeros(num_of_dump)  # 卸载点矿石品位上限

        # 装/卸区的物料类型
        self.load_area_material_type = {}
        self.unload_area_material_type = {}

    # 设置卸载点信息
    def extract_dump_info(self):
        try:
            rule3 = session_mysql.query(DispatchRule).filter_by(id=3).first()
            if not rule3.disabled:
                for dump_index in range(dynamic_excavator_num):
                    unload_area_id = unload_area_index_to_uuid_dict[self.dump_index_to_unload_area_index_dict[dump_index]]

                    unload_ability = session_postgre.query(DumpArea).filter_by(Id=unload_area_id).first().UnloadAbililty
                    self.dump_strength[dump_index] = unload_ability # 卸载设备最大卸载能力，单位吨/小时

                    if unload_ability < 200:
                        raise Exception("卸载点卸载能力异常")
            else:
                self.dump_strength = np.full(self.num_of_dump, 5000)

            for dump_index in range(dynamic_excavator_num):
                # self.dump_strength[dump_index] = 10000  # 卸载设备最大卸载能力，单位吨/小时
                self.grade_upper_dump_array[dump_index] = 100  # 卸点品位上限
                self.grade_lower_dump_array[dump_index] = 100  # 卸点品位下限
                self.dump_priority_coefficient[dump_index] = 1  # 卸载设备优先级

        except Exception as es:
            logger.error(es)
            logger.error("卸载点信息设置异常")

    # # 提取挖机信息并建立映射
    # def extract_excavator_info(self):
    #     try:
    #         excavator_index = 0
    #         for dispatch in session_mysql.query(Dispatch).filter_by(isdeleted=0, isauto=1).all():
    #             excavator_id = dispatch.exactor_id
    #             load_area_id = dispatch.load_area_id
    #             if excavator_id not in self.excavator_uuid_to_index_dict:
    #                 # excavator_uuid <-> excavator_uuid
    #                 self.excavator_uuid_to_index_dict[excavator_id] = excavator_index
    #                 self.excavator_index_to_uuid_dict[excavator_index] = excavator_id
    #                 # excavator_uuid -> load_area_uuid
    #                 self.excavator_uuid_to_load_area_uuid_dict[excavator_id] = load_area_id
    #                 # excavator_id -> load_area_id
    #                 self.excavator_index_to_load_area_index_dict[
    #                     self.excavator_uuid_to_index_dict[excavator_id]] = \
    #                     load_area_uuid_to_index_dict[load_area_id]
    #
    #                 self.excavator_strength[excavator_index] = 300  # 挖机最大装载能力，单位吨/小时
    #                 self.grade_loading_array[excavator_index] = 100  # 挖机装载物料品位
    #                 self.excavator_priority_coefficient[excavator_index] = 1  # 挖机优先级
    #                 excavator_index += 1
    #     except Exception as es:
    #         logger.error("车流规划读取挖机信息异常")

    # 设置挖信息
    def extract_excavator_info(self):
        try:
            rule4 = session_mysql.query(DispatchRule).filter_by(id=4).first()
            if not rule4.disabled:
                for excavator_index in range(len(self.excavator_index_to_uuid_dict)):
                    load_ability = session_mysql.query(EquipmentSpec.mining_abililty).\
                        join(Equipment, Equipment.equipment_spec == EquipmentSpec.id).\
                        filter(Equipment.id == self.excavator_index_to_uuid_dict[excavator_index]).first()
                    self.excavator_strength[excavator_index] = load_ability.mining_abililty

                    if load_ability.mining_abililty < 200:
                        raise Exception("挖机装载能力异常")
            else:
                self.excavator_strength = np.full(self.num_of_excavator, 5000)
            for excavator_index in range(len(self.excavator_index_to_uuid_dict)):
                # self.excavator_strength[excavator_index] = 1000  # 挖机最大装载能力，单位吨/小时
                self.grade_loading_array[excavator_index] = 100  # 挖机装载物料品位
                self.excavator_priority_coefficient[excavator_index] = 1  # 挖机优先级

        except Exception as es:
            logger.error(es)
            logger.error("挖机信息设置异常")

    # def extract_walk_time_info(self):
    #     # load_area_uuid <-> load_area_id
    #     # unload_area_uuid <-> unload_area_id
    #     load_area_index = 0
    #     unload_area_index = 0
    #     for walk_time in session_postgre.query(WalkTime).all():
    #
    #         load_area_id = str(walk_time.load_area_id)
    #         unload_area_id = str(walk_time.unload_area_id)
    #
    #         if load_area_id not in load_area_uuid_to_index_dict:
    #             load_area_uuid_to_index_dict[load_area_id] = load_area_index
    #             load_area_index_to_uuid_dict[load_area_index] = load_area_id
    #             load_area_index += 1
    #         if unload_area_id not in unload_area_uuid_to_index_dict:
    #             unload_area_uuid_to_index_dict[unload_area_id] = unload_area_index
    #             unload_area_index_to_uuid_dict[unload_area_index] = unload_area_id
    #             unload_area_index += 1



    # 根据物料优先级生成新的影响调度的coast矩阵
    # def arrange_material_type (self, material_type):
    #
    #     # 首先判断物料的种
    #     # if material_type:
    #     #     logger.info(f'物料类型是土方')
    #     # else：
    #     #     logger.info(f'物料类型是煤方')

    # def extract_excavator_priority(self):
    #     for excavator_id in dynamic_excavator_set:
    #         item = session_mysql.query(Equipment).filter_by(id=excavator_id).first()
    #         self.excavator_priority_coefficient[self.excavator_uuid_to_index_dict[excavator_id]] = item.priority + 1
    #
    #         self.excavator.update_excavator_material()
    #
    #         # 物料优先级控制
    #         rule = 1
    #         rule7 = session_mysql.query(DispatchRule).filter_by(id=7).first()
    #         material_priority_use = rule7.disabled
    #         if material_priority_use == 0:
    #             rule = rule7.rule_weight
    #
    #         if rule == 0:
    #             if self.excavator.excavator_material[excavator_id] == 'c8092d59-7597-44d7-a731-5a568b46060e':
    #                 print("here111")
    #                 self.excavator_material_priority[self.excavator_uuid_to_index_dict[excavator_id]] = 5
    #         elif rule == 2:
    #             if self.excavator.excavator_material[excavator_id] == 'c481794b-6ced-45b9-a9c4-c4a388f44418':
    #                 self.excavator_material_priority[self.excavator_uuid_to_index_dict[excavator_id]] = 5
    #
    #     print("挖机优先级")
    #     print(self.excavator_material_priority)
    #
    # def extract_dump_priority(self):
    #     for excavator_id in dynamic_excavator_set:
    #         item = session_mysql.query(Equipment).filter_by(id=excavator_id).first()
    #         self.excavator_priority_coefficient[self.excavator_uuid_to_index_dict[excavator_id]] = item.priority + 1

    # def extract_walk_time_info(self, include_material_type):
    #
    #     # try:
    #     # 车流规划部分矩阵格式与其余两个模块不同
    #     cost_to_load_area = self.path_planner.cost_to_load_area
    #     cost_to_unload_area = self.path_planner.cost_to_unload_area
    #
    #     distance_to_load_area = self.path_planner.distance_to_load_area
    #     distance_to_unload_area = self.path_planner.distance_to_unload_area
    #
    #     self.load_area_material_type = {}
    #     self.unload_area_material_type = {}
    #     for item in session_postgre.query(DiggingWorkArea).all():
    #         load_area_id = str(item.Id)
    #         # if load_area_id in load_area_uuid_to_index_dict:
    #         self.load_area_material_type[load_area_id] = item.Material
    #
    #     for item in session_postgre.query(DumpArea).all():
    #         unload_area_id = str(item.Id)
    #         # if unload_area_id in unload_area_uuid_to_index_dict:
    #         self.unload_area_material_type[unload_area_id] = item.Material
    #
    #
    #     # # 物料优先级控制
    #     # factor = 1
    #     # rule = 1
    #     # rule7 = session_mysql.query(DispatchRule).filter_by(id=7).first()
    #     # material_priority_use = rule7.disabled
    #     # if material_priority_use == 0:
    #     #     rule = rule7.rule_weight
    #     #
    #     # # 路网信息读取
    #     # for unload_area_index in range(unload_area_num):
    #     #     for load_area_index in range(load_area_num):
    #     #         unload_area_id = unload_area_index_to_uuid_dict[unload_area_index]
    #     #         load_area_id = load_area_index_to_uuid_dict[load_area_index]
    #     #         if unload_area_id in unload_area_uuid_to_index_dict and load_area_id in load_area_uuid_to_index_dict:
    #     #
    #     #             print(cost_to_load_area[unload_area_index][load_area_index], (empty_speed * 1000), self.payload)
    #     #
    #     #             if rule == 0:
    #     #                 if self.load_area_material_type[load_area_id] == 'c8092d59-7597-44d7-a731-5a568b46060e':
    #     #                     self.goto_load_area_factor[unload_area_index][load_area_index] = \
    #     #                          5 * (cost_to_load_area[unload_area_index][load_area_index] / (empty_speed * 1000)) / self.payload
    #     #                 else:
    #     #                     self.goto_load_area_factor[unload_area_index][load_area_index] = \
    #     #                         (cost_to_load_area[unload_area_index][load_area_index] / (empty_speed * 1000)) / self.payload
    #     #
    #     #             elif rule == 1:
    #     #                 self.goto_load_area_factor[unload_area_index][load_area_index] = \
    #     #                     (cost_to_load_area[unload_area_index][load_area_index] / (empty_speed * 1000)) / self.payload
    #     #
    #     #             elif rule == 2:
    #     #                 if self.load_area_material_type[load_area_id] == 'c481794b-6ced-45b9-a9c4-c4a388f44418':
    #     #                     self.goto_load_area_factor[unload_area_index][load_area_index] = \
    #     #                         5 * (cost_to_load_area[unload_area_index][load_area_index] / (empty_speed * 1000)) / self.payload
    #     #                 else:
    #     #                     self.goto_load_area_factor[unload_area_index][load_area_index] = \
    #     #                         (cost_to_load_area[unload_area_index][load_area_index] / (empty_speed * 1000)) / self.payload
    #     #
    #     #             self.goto_unload_area_factor[load_area_index][unload_area_index] = \
    #     #                 (cost_to_unload_area[unload_area_index][load_area_index] / (heavy_speed * 1000)) / self.payload

    def extract_walk_time_info(self):
        try:
            # 车流规划部分矩阵格式与其余两个模块不同
            cost_to_load_area = self.path_planner.cost_to_load_area
            cost_to_unload_area = self.path_planner.cost_to_unload_area

            distance_to_load_area = self.path_planner.distance_to_load_area
            distance_to_unload_area = self.path_planner.distance_to_unload_area

            # 路网信息读取
            for unload_area_index in range(unload_area_num):
                for load_area_index in range(load_area_num):
                    self.goto_load_area_factor[unload_area_index][load_area_index] = \
                        (cost_to_load_area[unload_area_index][load_area_index] / (empty_speed * 1000)) / self.payload

                    self.goto_unload_area_factor[load_area_index][unload_area_index] = \
                        (cost_to_unload_area[unload_area_index][load_area_index] / (heavy_speed * 1000)) / self.payload
        except Exception as es:
            logger.error(es)
            logger.error("车流规划信息计算异常")


# 初始化车流规划类
def Traffic_para_init(num_of_load_area, num_of_unload_area, num_of_excavator, num_of_dump):

    # try:

    tra_para = Traffic_para(num_of_load_area, num_of_unload_area, num_of_excavator, num_of_dump)

    tra_para.period_map_para_load()

    tra_para.period_walk_para_load()

    tra_para.extract_excavator_info()

    tra_para.extract_dump_info()

    tra_para.extract_walk_time_info()

    tra_para.truck.update_truck_payload()

    tra_para.payload = np.mean(tra_para.truck.get_payload())

    # 全部矿卡设备集合
    truck_set = set(update_total_truck())

    # 固定派车矿卡集合
    fixed_truck_set = set(update_fixdisp_truck())

    # 动态派车矿卡集合
    tra_para.truck_total_num = len(truck_set.difference(fixed_truck_set))

    # 计算逻辑道路因子
    for i in range(num_of_excavator):
        for j in range(num_of_dump):
            # 查找挖机绑定的装载区, 卸载设备绑定的卸载区
            load_area_index = tra_para.excavator_index_to_load_area_index_dict[i]
            unload_area_index = tra_para.dump_index_to_unload_area_index_dict[j]

            # 逻辑道路因子赋值, 来自实际道路因子
            tra_para.goto_excavator_factor[j][i] = \
                tra_para.goto_load_area_factor[unload_area_index][load_area_index]

            tra_para.goto_dump_factor[i][j] = \
                tra_para.goto_unload_area_factor[load_area_index][unload_area_index]

            # # 设备优先级
            # if not device_priority_use:
            #     print("here1")
            #     # 每条卸载道路的优先级,等于电铲的优先级乘以卸载点的优先级
            #     tra_para.priority_coefficient_goto_dump[i][j] = tra_para.excavator_priority_coefficient[i] \
            #                                                         * tra_para.dump_priority_coefficient[j]
            #
            #     # 每条装载道路的优先级,等于电铲的优先级乘以卸载点的优先级
            #     tra_para.priority_coefficient_goto_excavator[j][i] = tra_para.excavator_priority_coefficient[i] \
            #                                                         * tra_para.dump_priority_coefficient[j]
            # # 物料优先级
            # if not material_priority_use:
            #     print("here2")
            #     # # 每条卸载道路的优先级,等于电铲的优先级乘以卸载点的优先级
            #     # tra_para.priority_coefficient_goto_dump[i][j] += tra_para.excavator_material_priority[i] \
            #     #                                                     * tra_para.dump_material_priority[j]
            #
            #     # 每条装载道路的优先级,等于电铲的优先级乘以卸载点的优先级
            #     print(tra_para.excavator_material_priority[i], tra_para.dump_material_priority)
            #     tra_para.priority_coefficient_goto_excavator[j][i] = tra_para.excavator_material_priority[i] \
            #                                                         * tra_para.dump_material_priority[j]


            # # 逻辑距离赋值，来自实际道路距离
            # tra_para.walk_time_to_excavator[j][i] = \
            #     tra_para.walk_time_to_load_area[unload_area_index][load_area_index]
            #
            # tra_para.walk_time_to_dump[i][j] = \
            #     tra_para.walk_time_to_unload_area[load_area_index][unload_area_index]

    print("cout", tra_para.priority_coefficient_goto_dump, tra_para.priority_coefficient_goto_excavator)

    # except Exception as es:
    #     logger.error(es)
    #     logger.error("车流规划类比初始化异常")
    return tra_para
