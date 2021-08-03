#!E:\Pycharm Projects\Waytous
# -*- coding: utf-8 -*-
# @Time : 2021/6/22 12:46
# @Author : Opfer
# @Site :
# @File : tables.py
# @Software: PyCharm


# 存储数据库表结构


from sqlalchemy import Column, create_engine
from sqlalchemy import VARCHAR, DateTime, Float, Integer, BOOLEAN
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import numpy as np
from redis import StrictRedis, ConnectionPool
import redis
from datetime import datetime, timedelta
import copy
import json
import sched
import time

pool5 = ConnectionPool(host='192.168.28.111', db=5, port=6379, password='Huituo@123')

redis5 = StrictRedis(connection_pool=pool5)

pool2 = ConnectionPool(host='192.168.28.111', db=2, port=6379, password='Huituo@123')

redis2 = StrictRedis(connection_pool=pool2)

# 创建对象的基类:
Base = declarative_base()


# 初始化数据库连接:
engine_mysql = create_engine('mysql+mysqlconnector://root:Huituo@123@192.168.28.111:3306/waytous')

engine_postgre = create_engine('postgresql://postgres:Huituo@123@192.168.28.111:5432/shenbao_2021520')

# 创建DBsession_mysql类型:
DBsession_mysql = sessionmaker(bind=engine_mysql)

DBsession_postgre = sessionmaker(bind=engine_postgre)

# 创建session_mysql对象:
session_mysql = DBsession_mysql()

session_postgre = DBsession_postgre()


# 定义对象:
# class Excavator(Base):
#     __tablename__ = 'excavator_property'
#
#     excavator_id = Column(VARCHAR(36), primary_key=True)
#     load_area_id = Column(VARCHAR(36))
#     average_load_time = Column(Integer)
#     target_mass = Column(Float(2))
#     actual_mass = Column(Float(2))
#     # virtual_mass = Column(Float(2))
#     # last_load_time = Column(DateTime)
#
#     def __init__(self, excavator_id, load_area_id, average_load_time, target_mass, actual_mass):
#         self.excavator_id = excavator_id
#         self.load_area_id = load_area_id
#         self.average_load_time = average_load_time
#         self.target_mass = target_mass
#         self.actual_mass = actual_mass
#         # self.virtual_mass = virtual_mass
#         # self.last_load_time = last_load_time


# class Dump(Base):
#     __tablename__ = 'dump_property'
#
#     dump_id = Column(VARCHAR(36), primary_key=True)
#     unload_area_id = Column(VARCHAR(36))
#     average_unload_time = Column(Integer)
#     target_mass = Column(Float(2))
#     actual_mass = Column(Float(2))
#     # virtual_mass = Column(Float(2))
#     # last_unload_time = Column(DateTime)
#
#     def __init__(self, dump_id, unload_area_id, average_unload_time, target_mass, actual_mass):
#         self.dump_id = dump_id
#         self.unload_area_id = unload_area_id
#         self.average_unload_time = average_unload_time
#         self.target_mass = target_mass
#         self.actual_mass = actual_mass
#         # self.virtual_mass = virtual_mass
#         # self.last_unload_time = last_unload_time


# class Walk_time(Base):
#     __tablename__ = 'walk_time'
#
#     Rid = Column(VARCHAR(36), primary_key=True)
#     load_area_id = Column(VARCHAR(36))
#     unload_area_id = Column(VARCHAR(36))
#     walktime_load = Column(Float(2))
#     walktime_unload = Column(Float(2))
#
#     def __init__(self, Rid, load_area_id, unload_area_id, walktime_load, walktime_unload):
#         self.Rid = Rid
#         self.load_area_id = load_area_id
#         self.unload_area_id = unload_area_id
#         self.walktime_load = walktime_load
#         self.walktime_unload = walktime_unload

class WalkTime(Base):
    __tablename__ = 'work_area_distance'

    load_area_id = Column(VARCHAR(36), primary_key=True)
    unload_area_id = Column(VARCHAR(36), primary_key=True)
    load_area_name = Column(VARCHAR(30))
    unload_area_name = Column(VARCHAR(30))
    to_unload_distance = Column(Float(10))
    to_load_distance = Column(Float(10))
<<<<<<< HEAD
    to_unload_lanes = Column(VARCHAR(100))
    to_load_lanes = Column(VARCHAR(100))

    def __init__(self, load_area_id, unload_area_id, load_area_name, unload_area_name, to_load_distance,
                 to_unload_distance, to_unload_lanes, to_load_lanes):
=======

    def __init__(self, load_area_id, unload_area_id, load_area_name, unload_area_name, to_load_distance,
                 to_unload_distance):
>>>>>>> acc639d456bb6cab81fb693f04b04a95a6f30b81
        self.load_area_id = load_area_id
        self.unload_area_id = unload_area_id
        self.load_area_name = load_area_name
        self.unload_area_name = unload_area_name
        self.to_load_distance = to_load_distance
        self.to_unload_distance = to_unload_distance
<<<<<<< HEAD
        self.to_unload_lanes = to_unload_lanes
        self.to_load_lanes = to_load_lanes
=======
>>>>>>> acc639d456bb6cab81fb693f04b04a95a6f30b81

    # Rid = Column(VARCHAR(36), primary_key=True)
    # load_area_id = Column(VARCHAR(36))
    # unload_area_id = Column(VARCHAR(36))
    # walktime_load = Column(Float(2))
    # walktime_unload = Column(Float(2))
    #
    # def __init__(self, Rid, load_area_id, unload_area_id, walktime_load, walktime_unload):
    #     self.Rid = Rid
    #     self.load_area_id = load_area_id
    #     self.unload_area_id = unload_area_id
    #     self.walktime_load = walktime_load
    #     self.walktime_unload = walktime_unload


# class Truck(Base):
#     __tablename__ = 'truck_status'
#
#     truck_id = Column(VARCHAR(36), primary_key=True)
#     # dispatch_id = Column(VARCHAR(36))
#     current_task = Column(Integer)
#     # reach_time = Column(DateTime)
#     last_load_time = Column(DateTime)
#     last_unload_time = Column(DateTime)
#     payload = Column(Float(2))
#
#     def __init__(self, truck_id, current_task, last_load_time, last_unload_time, payload):
#         self.truck_id = truck_id
#         # self.dispatch_id = dispatch_id
#         self.current_task = current_task
#         # self.reach_time = reach_time
#         self.last_load_time = last_load_time
#         self.last_unload_time = last_unload_time
#         self.payload = payload
#
#     def check_existing(self):
#         existing = session_mysql.query(Truck).filter_by(truck_id=self.truck_id).first()
#         if not existing:
#             truck = Truck(self.truck_id, self.dispatch_id, self.status, self.reach_time, self.last_load_time, self.last_unload_time, self.payload)
#         else:
#             truck = existing
#         session_mysql.close()
#         return truck


# class Dispatch(Base):
#     __tablename__ = 'dispatch_pair'
#
#     dispatch_id = Column(VARCHAR(36), primary_key=True)
#     excavator_id = Column(VARCHAR(36))
#     dump_id = Column(VARCHAR(36))
#
#     def __init__(self, dispatch_id, excavator_id, dump_id):
#         self.dispatch_id = dispatch_id
#         self.excavator_id = excavator_id
#         self.dump_id = dump_id

class EquipmentPair(Base):
    __tablename__ = 'sys_equipment_pair'

    id = Column(VARCHAR(36), primary_key=True)
    truck_id = Column(VARCHAR(36))
    exactor_id = Column(VARCHAR(36))
    dump_id = Column(VARCHAR(36))
    load_area_id = Column(VARCHAR(36))
    unload_area_id = Column(VARCHAR(36))
    dispatch_id = Column(VARCHAR(36))
    isdeleted = Column(BOOLEAN)
    createtime = Column(DateTime)

    def __init__(self, id, truck_id, exactor_id, dump_id, load_area_id, unload_area_id, dispatch_id, isdeleted, createtime):
        self.id = id
        self.truck_id = truck_id
        self.exactor_id = exactor_id
        self.dump_id = dump_id
        self.load_area_id = load_area_id
        self.unload_area_id = unload_area_id
        self.dispatch_id = dispatch_id
        self.isdeleted = isdeleted
        self.createtime = createtime

<<<<<<< HEAD
# class Lane(Base):
#     # 表的名字
#     __tablename__ = 'Geo_Node'
#     Id = Column(VARCHAR(36), primary_key=True)
#     LaneIds = Column(VARCHAR(100))
#
#     def __init__(self, Id, LaneIds):
#         self.Id = Id
#         self.LaneIds = LaneIds

class Lane(Base):
    # 表的名字
    __tablename__ = 'Geo_Lane'
    Id = Column(VARCHAR(36), primary_key=True)
    Length = Column(Float)
    MaxSpeed = Column(Float)

    def __init__(self, Id, Length, MaxSpeed):
        self.Id = Id
        self.Length = Length
        self.MaxSpeed = MaxSpeed

=======
>>>>>>> acc639d456bb6cab81fb693f04b04a95a6f30b81
class Dispatch(Base):
    # 表的名字:
    __tablename__ = 'sys_dispatch_setting'

    id = Column(VARCHAR(36), primary_key=True)
    load_area_id = Column(VARCHAR(36))
    exactor_id = Column(VARCHAR(36))
    unload_area_id = Column(VARCHAR(36))
    dump_id = Column(VARCHAR(36))
    isauto = Column(BOOLEAN)
    truck_id = Column(VARCHAR(36))
    remark = Column(VARCHAR(100))
    proportion_id = Column(VARCHAR(36))
    isdeleted = Column(BOOLEAN)
    creator = Column(VARCHAR(36))
    createtime = Column(DateTime)
    updator = Column(VARCHAR(36))
    updatetime = Column(DateTime)
    deletor = Column(VARCHAR(36))
    deletetime = Column(DateTime)

    def __init__(self, id, load_area_id, exactor_id, unload_area_id, dump_id, isauto, truck_id, remark, proportion_id,
                 isdeleted, creator, createtime, updator, updatetime, deletor, deletetime):
        self.id = id
        self.load_area_id = load_area_id
        self.excavator_id = exactor_id
        self.unload_area_id = unload_area_id
        self.dump_id = dump_id
        self.isauto = isauto
        self.truck_id = truck_id
        self.remark = remark
        self.proportion_id = proportion_id
        self.isdeleted = isdeleted
        self.creator = creator
        self.createtime = createtime
        self.updator = updator
        self.updatetime = updatetime
        self.deletor = deletor
        self.deletetime = deletetime

<<<<<<< HEAD
class WalkTimePark(Base):
=======
class WalkTimePort(Base):
>>>>>>> acc639d456bb6cab81fb693f04b04a95a6f30b81
    __tablename__ = 'park_load_distance'

    park_area_id = Column(VARCHAR(36), primary_key=True)
    load_area_id = Column(VARCHAR(36), primary_key=True)
    park_area_name = Column(VARCHAR(36))
    load_area_name = Column(VARCHAR(36))
    park_load_distance = Column(Float(10))
<<<<<<< HEAD
    park_load_lanes = Column(VARCHAR(100))

    def __init__(self, park_area_id, load_area_id, park_area_name, load_area_name, park_load_distance, park_load_lanes):
=======

    def __init__(self, park_area_id, load_area_id, park_area_name, load_area_name, park_load_distance):
>>>>>>> acc639d456bb6cab81fb693f04b04a95a6f30b81
        self.park_area_id = park_area_id
        self.load_area_id = load_area_id
        self.park_area_name = park_area_name
        self.load_area_name = load_area_name
        self.park_load_distance = park_load_distance
<<<<<<< HEAD
        self.park_load_lanes = park_load_lanes
=======
>>>>>>> acc639d456bb6cab81fb693f04b04a95a6f30b81

class Equipment(Base):
    __tablename__ = 'sys_equipment'

    id = Column(VARCHAR(36), primary_key=True)
    equipment_id = Column(VARCHAR(64))
    device_name = Column(VARCHAR(64))
    device_type = Column(VARCHAR(36))
    equipment_spec = Column(VARCHAR(36))
    equipment_state = Column(Integer)
    isdeleted = Column(Integer)

    def __init__(self, id, equipment_id, device_name, device_type, equipment_spec, equipment_state, isdeleted):
        self.id = id
        self.equipment_id = equipment_id
        self.device_name = device_name
        self.device_type = device_type
        self.equipment_spec = equipment_spec
        self.equipment_state = equipment_state
        self.isdeleted = isdeleted

class EquipmentSpec(Base):
    __tablename__ = 'sys_equipment_spec'

    id = Column(VARCHAR(36), primary_key=True)
    capacity = Column(Integer)

    def __init__(self, id, capacity):
        self.id = id
        self.capacity = capacity

class LoadInfo(Base):
    __tablename__ = 'sys_loadinfo'

    time = Column(DateTime, primary_key=True)
    dump_id = Column(VARCHAR(36), primary_key=True)
    load_weight = Column(Float(8))

    def __init__(self, time, dump_id, load_weight):
        self.time = time
        self.dump_id = dump_id
        self.load_weght = load_weight

class JobRecord(Base):
    __tablename__ = 'statistic_job_record'

    id = Column(VARCHAR(36), primary_key=True)
    equipment_id = Column(VARCHAR(50))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    work_type = Column(Integer)

    def __init__(self, id, equipment_id, start_time, end_time, work_type):
        self.id = id
        self.equipment_id = equipment_id
        self.start_time = start_time
        self.end_time = end_time
        self.work_type = work_type