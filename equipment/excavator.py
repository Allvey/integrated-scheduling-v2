#!E:\Pycharm Projects\Waytous
# -*- coding: utf-8 -*-
# @Time : 2021/8/24 11:28
# @Author : Opfer
# @Site :
# @File : excavator.py    
# @Software: PyCharm

from traffic_flow.traffic_flow_planner import *
from static_data_process import *
from para_config import *
from settings import *

# 挖机设备类
class ExcavatorInfo(WalkManage):
    def __init__(self):
        # # 挖机集合
        # self.dynamic_excavator_set = set(update_autodisp_excavator())
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
        # 挖机对应物料类型
        self.excavator_material = {}
        # 挖机设备优先级
        self.excavator_priority_coefficient = np.ones(dynamic_excavator_num)
        # 挖机物料优先级
        self.excavator_material_priority = np.ones(dynamic_excavator_num)

        # 初始化读取映射及路网
        self.period_map_para_load()
        self.period_walk_para_load()

        # 参数初始化
        self.para_period_update()

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
                for query in (
                    session_mysql.query(JobRecord.start_time, JobRecord.end_time)
                    .join(Equipment, JobRecord.equipment_id == Equipment.equipment_id)
                    .filter(Equipment.id == excavator_id, JobRecord.end_time != None)
                    .order_by(JobRecord.start_time.desc())
                    .limit(10)
                ):
                    ave_load_time = ave_load_time + float(
                        (query.end_time - query.start_time)
                        / timedelta(hours=0, minutes=1, seconds=0)
                    )
                    load_count = load_count + 1
                self.loading_time[self.excavator_uuid_to_index_dict[excavator_id]] = (
                    ave_load_time / load_count
                )
            except Exception as es:
                logger.error(f"挖机 {excavator_id} 装载时间信息缺失, 已设为默认值(1min)")
                logger.error(es)
                self.loading_time[
                    self.excavator_uuid_to_index_dict[excavator_id]
                ] = 5.00

    # 更新挖机设备出入时间
    def update_excavator_entrance_exit_time(self):
        self.entrance_time = np.zeros(self.dynamic_excavator_num)
        self.exit_time = np.zeros(self.dynamic_excavator_num)
        now = datetime.now().strftime("%Y-%m-%d")

        for excavator_id in self.excavator_uuid_to_index_dict.keys():
            try:
                for query in (
                    session_mysql.query(WorkRecord)
                    .filter(
                        WorkRecord.equipment_id == excavator_id,
                        WorkRecord.work_day > now,
                    )
                    .first()
                ):
                    self.entrance_time[
                        self.excavator_uuid_to_index_dict[excavator_id]
                    ] = float(query.load_entrance_time / query.load_entrance_count)
                    self.exit_time[
                        self.excavator_uuid_to_index_dict[excavator_id]
                    ] = float(query.exit_entrance_time / query.exit_entrance_count)
            except Exception as es:
                logger.error(f"挖机设备 {excavator_id} 出入场时间信息缺失, 已设为默认值(1min)")
                logger.error(es)
                self.entrance_time[
                    self.excavator_uuid_to_index_dict[excavator_id]
                ] = 0.50
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
        now = datetime.now().strftime("%Y-%m-%d")
        for excavator_id in self.excavator_uuid_to_index_dict.keys():
            # print(excavator_id)
            for query in (
                session_mysql.query(LoadInfo)
                .join(Equipment, LoadInfo.dump_id == Equipment.equipment_id)
                .filter(Equipment.id == excavator_id, LoadInfo.time > now)
                .order_by(LoadInfo.time.desc())
                .all()
            ):
                # print("time:", query.time)
                # print("load_weight:", )
                self.cur_excavator_real_mass[
                    self.excavator_uuid_to_index_dict[excavator_id]
                ] = (
                    self.cur_excavator_real_mass[
                        self.excavator_uuid_to_index_dict[excavator_id]
                    ]
                    + query.load_weight
                )

    def update_excavator_material(self):
        for excavator_id in dynamic_excavator_set:
            load_area_id = session_mysql.query(Dispatch).filter_by(exactor_id=excavator_id).first().load_area_id
            excavator_material_id = session_postgre.query(DiggingWorkArea).filter_by(Id=load_area_id).first().Material
            self.excavator_material[excavator_id] = excavator_material_id

    def update_excavator_priority(self):
        for excavator_id in dynamic_excavator_set:
            item = session_mysql.query(Equipment).filter_by(id=excavator_id).first()
            self.excavator_priority_coefficient[self.excavator_uuid_to_index_dict[excavator_id]] = item.priority + 1

            # 物料优先级控制
            rule = 2
            rule7 = session_mysql.query(DispatchRule).filter_by(id=7).first()
            material_priority_use = rule7.disabled
            if material_priority_use == 0:
                rule = rule7.rule_weight

            if rule == 3:
                if session_mysql.query(Material).filter_by(id=self.excavator_material[excavator_id]).first().name == '土':
                    self.excavator_material_priority[self.excavator_uuid_to_index_dict[excavator_id]] = 5
            elif rule == 1:
                if session_mysql.query(Material).filter_by(id=self.excavator_material[excavator_id]).first().name == '煤':
                    self.excavator_material_priority[self.excavator_uuid_to_index_dict[excavator_id]] = 5


    def para_period_update(self):

        # print("Excavator update!")

        logger.info("Excavator update!")

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

        # 更新挖机物料
        self.update_excavator_material()

        # 更新挖机优先级

        # 挖机目标产量
        self.excavator_target_mass = np.full(
            self.dynamic_excavator_num, excavator_target_mass
        )
