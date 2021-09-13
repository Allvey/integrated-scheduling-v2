#!E:\Pycharm Projects\Waytous
# -*- coding: utf-8 -*-
# @Time : 2021/9/3 14:44
# @Author : Opfer
# @Site :
# @File : priority_control.py
# @Software: PyCharm

from equipment.truck import *
from equipment.dump import *
from equipment.excavator import *
from para_config import *

truck = TruckInfo()
excavator = ExcavatorInfo()
dump = DumpInfo()


def weighted_walk_cost():
    excavator.update_excavator_priority()
    dump.update_dump_priority()
    walk_weight = np.ones((dynamic_dump_num, dynamic_excavator_num))
    excavator_priority = excavator.excavator_priority_coefficient
    excavator_material_priority = excavator.excavator_material_priority
    dump_priority = dump.dump_priority_coefficient
    dump_material_priority = np.ones(dynamic_dump_num)
    park_walk_weight = np.ones((park_num, dynamic_excavator_num))

    rule6 = session_mysql.query(DispatchRule).filter_by(id=6).first()

    if not rule6.disabled:
        for dump_id in dynamic_dump_set:
            for excavator_id in dynamic_excavator_set:
                dump_index = dump.dump_uuid_to_index_dict[dump_id]
                excavator_inedx = excavator.excavator_uuid_to_index_dict[excavator_id]
                walk_weight[dump_index][excavator_inedx] += dump_priority[dump_index] * \
                                                           excavator_priority[excavator_inedx]
        park_walk_weight = park_walk_weight * excavator.excavator_priority_coefficient

    rule7 = session_mysql.query(DispatchRule).filter_by(id=7).first()

    if not rule7.disabled:
        for dump_id in dynamic_dump_set:
            for excavator_id in dynamic_excavator_set:
                dump_index = dump.dump_uuid_to_index_dict[dump_id]
                excavator_inedx = excavator.excavator_uuid_to_index_dict[excavator_id]
                walk_weight[dump_index][excavator_inedx] += dump_material_priority[dump_index] * \
                                                           excavator_material_priority[excavator_inedx]
        park_walk_weight = park_walk_weight * excavator.excavator_material_priority

    walk_weight = walk_weight - (walk_weight.min() - 1)

    park_walk_weight = park_walk_weight - (park_walk_weight.min() - 1)

    return walk_weight, park_walk_weight


def available_walk():
    excavator.update_excavator_material()
    dump.update_dump_material()
    walk_weight = np.ones((dynamic_dump_num, dynamic_excavator_num))

    for dump_id in dynamic_dump_set:
        for excavator_id in dynamic_excavator_set:
            dump_index = dump.dump_uuid_to_index_dict[dump_id]
            excavator_inedx = excavator.excavator_uuid_to_index_dict[excavator_id]
            if excavator.excavator_material[excavator_id] != dump.dump_material[dump_id]:
                walk_weight[dump_index][excavator_inedx] += 1000

    return walk_weight

