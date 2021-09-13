#!E:\Pycharm Projects\Waytous
# -*- coding: utf-8 -*-
# @Time : 2021/8/24 11:28
# @Author : Opfer
# @Site :
# @File : dump.py    
# @Software: PyCharm

from para_config import *
from settings import *

# 卸载设备类
class DumpInfo(WalkManage):
    def __init__(self):
        # 卸载设备数量
        self.dynamic_dump_num = len(dynamic_dump_set)
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
        # 卸载点物料类型
        self.dump_material = {}
        # 卸点优先级
        self.dump_priority_coefficient = np.ones(self.dynamic_dump_num)

        # 初始化读取映射及路网
        self.period_map_para_load()
        self.period_walk_para_load()

        # 参数初始化
        self.para_period_update()

    def get_unloading_time(self):
        return self.unloading_time

    def get_dump_num(self):
        return self.dynamic_dump_num

    def get_dynamic_dump_set(self):
        return self.dynamic_dump_set

    def get_unloading_task_time(self):
        unloading_time = self.unloading_time

        dump_entrance_time = self.entrance_time

        dump_exit_time = self.exit_time

        unloading_task_time = unloading_time + dump_entrance_time + dump_exit_time

        return unloading_task_time

    # 更新卸载设备卸载时间
    def update_dump_unloadtime(self):
        self.unloading_time = np.zeros(self.dynamic_dump_num)

        for dump_id in self.dump_uuid_to_index_dict.keys():
            ave_unload_time = 0
            unload_count = 0
            try:
                for query in (
                    session_mysql.query(JobRecord.start_time, JobRecord.end_time)
                    .join(Equipment, JobRecord.equipment_id == Equipment.equipment_id)
                    .filter(Equipment.id == dump_id, JobRecord.end_time != None)
                    .order_by(JobRecord.start_time.desc())
                    .limit(10)
                ):
                    ave_unload_time = ave_unload_time + float(
                        (query.end_time - query.start_time)
                        / timedelta(hours=0, minutes=1, seconds=0)
                    )
                    unload_count = unload_count + 1
                self.unloading_time[self.dump_uuid_to_index_dict[dump_id]] = (
                    ave_unload_time / unload_count
                )
            except Exception as es:
                logger.error(f"卸载设备 {dump_id} 卸载时间信息缺失, 已设为默认值(1min)")
                logger.error(es)
                self.unloading_time[self.dump_uuid_to_index_dict[dump_id]] = 5.00
        # print("average_unload_time: ", self.unloading_time[self.dump_uuid_to_index_dict[dump_id]])

    # 更新卸载设备出入时间
    def update_dump_entrance_exit_time(self):
        self.entrance_time = np.zeros(self.dynamic_dump_num)
        self.exit_time = np.zeros(self.dynamic_dump_num)
        now = datetime.now().strftime("%Y-%m-%d")

        for dump_id in self.dump_uuid_to_index_dict.keys():
            try:
                for query in (
                    session_mysql.query(WorkRecord)
                    .filter(
                        WorkRecord.equipment_id == dump_id, WorkRecord.work_day > now
                    )
                    .first()
                ):
                    self.entrance_time[self.dump_uuid_to_index_dict[dump_id]] = float(
                        query.load_entrance_time / query.load_entrance_count
                    )
                    self.exit_time[self.dump_uuid_to_index_dict[dump_id]] = float(
                        query.exit_entrance_time / query.exit_entrance_count
                    )
            except Exception as es:
                logger.error(f"卸载设备 {dump_id} 出入场时间信息缺失, 已设为默认值(1min)")
                logger.error(es)
                self.entrance_time[self.dump_uuid_to_index_dict[dump_id]] = 0.50
            self.exit_time[self.dump_uuid_to_index_dict[dump_id]] = 0.50

    def update_dump_material(self):
        self.dump_material = {}
        for dump_id in dynamic_dump_set:
            unload_area_id = session_mysql.query(Dispatch).filter_by(dump_id=dump_id, isauto=1, isdeleted=0).first().unload_area_id
            dump_material_id = session_postgre.query(DumpArea).filter_by(Id=unload_area_id).first().Material
            self.dump_material[dump_id] = dump_material_id

    def update_dump_priority(self):
        self.dump_priority_coefficient = np.ones(self.dynamic_dump_num)
        for dump_id in dynamic_dump_set:
            unload_area_index = self.dump_index_to_unload_area_index_dict[self.dump_uuid_to_index_dict[dump_id]]
            unload_area_id = unload_area_index_to_uuid_dict[unload_area_index]
            item = session_postgre.query(DumpArea).filter_by(Id=unload_area_id).first()
            self.dump_priority_coefficient[self.dump_uuid_to_index_dict[dump_id]] += item.Priority

    def para_period_update(self):

        # print("Dump update!")

        logger.info("Dump update!")

        # 装载周期参数
        self.period_map_para_load()

        self.period_walk_para_load()

        # 用于动态调度的卸载设备
        self.dynamic_dump_set = set(update_autodisp_dump())

        self.dynamic_dump_num = len(self.dynamic_dump_set)

        # 计算平均卸载时间
        self.update_dump_unloadtime()

        # 计算平均进出场时间
        self.update_dump_entrance_exit_time()

        # 更新卸点物料
        self.update_dump_material()

        # 更新设备优先级
        self.update_dump_priority()

        # 卸载目标产量
        self.dump_target_mass = np.full(self.dynamic_dump_num, dump_target_mass)