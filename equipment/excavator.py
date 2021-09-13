#!E:\Pycharm Projects\Waytous
# -*- coding: utf-8 -*-
# @Time : 2021/8/24 11:28
# @Author : Opfer
# @Site :
# @File : excavator.py    
# @Software: PyCharm

from para_config import *
from settings import *

# 挖机设备类
class ExcavatorInfo(WalkManage):
    def __init__(self):
        # # 挖机集合
        # self.dynamic_excavator_set = set(update_autodisp_excavator())
        # 装载设备数量
        self.dynamic_excavator_num = len(dynamic_excavator_set)
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
                self.loading_time[self.excavator_uuid_to_index_dict[excavator_id]] = 5.00

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

    def update_excavator_material(self):
        for excavator_id in dynamic_excavator_set:
            load_area_id = session_mysql.query(Dispatch).filter_by(exactor_id=excavator_id, isdeleted=0, isauto=1).first().load_area_id
            excavator_material_id = session_postgre.query(DiggingWorkArea).filter_by(Id=load_area_id).first().Material
            self.excavator_material[excavator_id] = excavator_material_id

    def update_excavator_priority(self):

        self.excavator_material_priority = np.ones(dynamic_excavator_num)

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
                if self.excavator_material[excavator_id] == 'c8092d59-7597-44d7-a731-5a568b46060e':
                    self.excavator_material_priority[self.excavator_uuid_to_index_dict[excavator_id]] = 5
            elif rule == 1:
                if self.excavator_material[excavator_id] == 'c481794b-6ced-45b9-a9c4-c4a388f44418':
                    self.excavator_material_priority[self.excavator_uuid_to_index_dict[excavator_id]] = 5


    def para_period_update(self):

        logger.info("Excavator update!")

        # 装载周期参数
        self.period_map_para_load()

        self.period_walk_para_load()

        # 用于动态调度的挖机设备
        self.dynamic_excavator_set = set(update_autodisp_excavator())

        self.dynamic_excavator_num = len(self.dynamic_excavator_set)

        # 计算平均装载时间
        self.update_excavator_loadtime()

        # 更新挖机物料
        self.update_excavator_material()

        # 更新挖机优先级
        self.update_excavator_priority()
