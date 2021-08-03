#!E:\Pycharm Projects\Waytous
# -*- coding: utf-8 -*-
# @Time : 2021/7/19 15:05
# @Author : Opfer
# @Site :
# @File : traffic_flow_planner.py
# @Software: PyCharm

# import
import numpy as np
import pulp
from tables import *
from urllib.parse import quote
import logging
import logging.handlers
from static_data_process import *
from settings import *

# 需要提供的值
# traffic_programme_para.excavator_strength[excavator_index] = 200  # 挖机最大装载能力，单位吨/小时
# traffic_programme_para.dump_strength[dump_index] = 200  # 卸载设备最大卸载能力，单位吨/小时

# traffic_programme_para.grade_loading_array[excavator_index] = 100  # 挖机装载物料品位

# traffic_programme_para.excavator_priority_coefficient[excavator_index] = 1  # 挖机优先级
# traffic_programme_para.dump_priority_coefficient[dump_index] = 1  # 卸载设备优先级

# traffic_programme_para.grade_upper_dump_array[dump_index] = 100  # 卸点品位上限
# traffic_programme_para.grade_lower_dump_array[dump_index] = 100  # 卸点品位下限




class TrafficProgPara(object):
    def __init__(self, num_of_load_area, num_of_unload_area, num_of_excavator, num_of_dump):
        self.load_area_uuid_to_index_dict = {}  # 用于保存装载点uuid到id的映射
        self.load_area_index_to_uuid_dict = {}  # 用于保存装载点id到uuid的映射
        self.unload_area_uuid_to_index_dict = {}  # 用于保存卸载点uuid到id的映射
        self.unload_area_index_to_uuid_dict = {}  # 用于保存卸载点id到uuid的映射

        self.excavator_uuid_to_index_dict = {}  # 用于保存挖机uuid到id的映射
        self.excavator_index_to_uuid_dict = {}  # 用于保存挖机id到uuid的映射
        self.dump_uuid_to_index_dict = {}  # 用于保存卸点uuid到id的映射
        self.dump_index_to_uuid_dict = {}  # 用于保存卸点id到uuid的映射

        self.dump_uuid_to_unload_area_uuid_dict = {}  # 用于保存卸点与卸载区的绑定关系(uuid)
        self.excavator_uuid_to_load_area_uuid_dict = {}  # 用于保存挖机与装载区的绑定关系(uuid)
        self.dump_index_to_unload_area_index_dict = {}  # 用于保存卸点与卸载区的绑定关系(id)
        self.excavator_index_to_load_area_index_dict = {}  # 用于保存挖机与装载区的绑定关系(id)

        self.excavator_strength = np.zeros(num_of_excavator)  # 用于保存挖机的工作强度,单位是t/h
        self.excavator_priority_coefficient = np.zeros(num_of_excavator)  # 每个挖机的优先级系数
        self.grade_loading_array = np.zeros(num_of_excavator)  # 用于保存挖机挖掘矿石的品位
        self.dump_strength = np.zeros(num_of_dump)  # 卸载点的工作强度，单位是t/h
        self.dump_priority_coefficient = np.zeros(num_of_dump)  # 每个卸载点的优先级系数

        # 装载道路上，每提供1吨的装载能力需要一辆卡车运行时长,等于（该装载道路上车辆平均运行时长/卡车平均装载能力）
        self.goto_unload_area_factor = np.full((num_of_load_area, num_of_unload_area), 10, dtype=float)  # 卸载道路的运输系数
        self.goto_unload_point_factor = np.full((num_of_excavator, num_of_dump), 10, dtype=float)  # 逻辑卸载道路的运输系数
        # 卸载道路上，每运输1吨货物需要一辆卡车运行时长,等于（该卸载道路上车辆平均运行时长/卡车平均实际装载量）
        self.goto_load_area_factor = np.full((num_of_unload_area, num_of_load_area), 10, dtype=float)  # 装载道路的运输系数
        self.goto_excavator_factor = np.full((num_of_dump, num_of_excavator), 10, dtype=float)  # 逻辑装载道路的运输系数

        self.priority_coefficient = np.zeros((num_of_excavator, num_of_dump))  # 卸载道路的优先级系数
        self.grade_lower_dump_array = np.zeros(num_of_dump)  # 卸载点矿石品位下限
        self.grade_upper_dump_array = np.zeros(num_of_dump)  # 卸载点矿石品位上限

        self.empty_speed = 25  # 空载矿卡平均时速
        self.heavy_speed = 22  # 重载矿卡平均时速
        self.goto_load_area_distance = np.zeros((num_of_unload_area, num_of_load_area))  # 空载运输路线距离
        self.goto_unload_area_distance = np.zeros((num_of_load_area, num_of_unload_area))  # 重载运输路线距离
        # 装载道路权重因子
        # self.avg_goto_excavator_weight = np.zeros((num_of_load_area, num_of_unload_area))
        self.avg_goto_excavator_weight = np.full((num_of_load_area, num_of_unload_area), 1)
        # 卸载道路
        # self.avg_goto_unload_point_weight = np.zeros((num_of_load_area, num_of_unload_area))
        self.avg_goto_unload_point_weight = np.full((num_of_load_area, num_of_unload_area), 1)
        self.goto_excavator_distance = np.zeros((num_of_dump, num_of_excavator))  # 逻辑空载运输路线距离
        self.goto_dump_distance = np.zeros((num_of_excavator, num_of_dump))  # 逻辑重载运输路线距离
        self.payload = 100  # 有效载重(不同型号矿卡载重不同，这里暂时认为车队是同质的)
        self.min_throughout = 200  # 最小产量约束
        self.truck_total_num = 0


def extract_excavator_info(traffic_programme_para):
    excavator_index = 0
    for dispatch in session_mysql.query(Dispatch).filter_by(isdeleted=0, isauto=1).all():
        excavator_id = dispatch.exactor_id
        load_area_id = dispatch.load_area_id
        if excavator_id not in traffic_programme_para.excavator_uuid_to_index_dict:
            # excavator_uuid <-> excavator_uuid
            traffic_programme_para.excavator_uuid_to_index_dict[excavator_id] = excavator_index
            traffic_programme_para.excavator_index_to_uuid_dict[excavator_index] = excavator_id
            # excavator_uuid -> load_area_uuid
            traffic_programme_para.excavator_uuid_to_load_area_uuid_dict[excavator_id] = load_area_id
            # excavator_id -> load_area_id
            traffic_programme_para.excavator_index_to_load_area_index_dict[
                traffic_programme_para.excavator_uuid_to_index_dict[excavator_id]] = \
                traffic_programme_para.load_area_uuid_to_index_dict[load_area_id]

            traffic_programme_para.excavator_strength[excavator_index] = 300  # 挖机最大装载能力，单位吨/小时
            traffic_programme_para.grade_loading_array[excavator_index] = 100  # 挖机装载物料品位
            traffic_programme_para.excavator_priority_coefficient[excavator_index] = 1  # 挖机优先级
            excavator_index = excavator_index + 1


def extract_dump_info(traffic_programme_para):
    dump_index = 0
    for dispatch in session_mysql.query(Dispatch).filter_by(isdeleted=0, isauto=1).all():
        unload_area_id = dispatch.unload_area_id
        dump_id = dispatch.dump_id
        if dump_id not in traffic_programme_para.dump_uuid_to_unload_area_uuid_dict:
            # dump_uuid <-> dump_id
            traffic_programme_para.dump_uuid_to_index_dict[dump_id] = dump_index
            traffic_programme_para.dump_index_to_uuid_dict[dump_index] = dump_id
            # dump_uuid -> unload_area_uuid
            traffic_programme_para.dump_uuid_to_unload_area_uuid_dict[dump_id] = unload_area_id
            # dump_id -> unload_area_id
            traffic_programme_para.dump_index_to_unload_area_index_dict[
                traffic_programme_para.dump_uuid_to_index_dict[dump_id]] = \
                traffic_programme_para.unload_area_uuid_to_index_dict[unload_area_id]

            traffic_programme_para.dump_strength[dump_index] = 300  # 卸载设备最大卸载能力，单位吨/小时
            traffic_programme_para.grade_upper_dump_array[dump_index] = 100  # 卸点品位上限
            traffic_programme_para.grade_lower_dump_array[dump_index] = 100  # 卸点品位下限
            traffic_programme_para.dump_priority_coefficient[dump_index] = 1  # 卸载设备优先级
            dump_index = dump_index + 1


def extract_walk_time_info(traffic_programme_para):
    # load_area_uuid <-> load_area_id
    # unload_area_uuid <-> unload_area_id
    load_area_index = 0
    unload_area_index = 0
    for walk_time in session_postgre.query(WalkTime).all():

        load_area_id = str(walk_time.load_area_id)
        unload_area_id = str(walk_time.unload_area_id)

        if load_area_id not in traffic_programme_para.load_area_uuid_to_index_dict:
            traffic_programme_para.load_area_uuid_to_index_dict[load_area_id] = load_area_index
            traffic_programme_para.load_area_index_to_uuid_dict[load_area_index] = load_area_id
            load_area_index = load_area_index + 1
        if unload_area_id not in traffic_programme_para.unload_area_uuid_to_index_dict:
            traffic_programme_para.unload_area_uuid_to_index_dict[unload_area_id] = unload_area_index
            traffic_programme_para.unload_area_index_to_uuid_dict[unload_area_index] = unload_area_id
            unload_area_index = unload_area_index + 1

    # 路网信息读取
    for walk_time in session_postgre.query(WalkTime).all():
        load_area_id = str(walk_time.load_area_id)
        unload_area_id = str(walk_time.unload_area_id)
        # 将uuid转为id
        load_area_index = traffic_programme_para.load_area_uuid_to_index_dict[load_area_id]
        unload_area_index = traffic_programme_para.unload_area_uuid_to_index_dict[unload_area_id]

        # 运输路线距离
        traffic_programme_para.goto_load_area_distance[unload_area_index][load_area_index] = walk_time.to_load_distance
        traffic_programme_para.goto_unload_area_distance[load_area_index][
            unload_area_index] = walk_time.to_unload_distance

        # 卸载道路上，每运输1吨货物需要一辆卡车运行时长,等于（该卸载道路上车辆平均运行时长/卡车平均实际装载量）
        # 单位为辆小时/吨
        # i代表第i个挖机,j代表第j个卸载点
        # walktime_goto_unload_point单位是秒，需要除以3600，转成小时
        traffic_programme_para.goto_load_area_factor[unload_area_index][load_area_index] = \
            (60 / 1000 * walk_time.to_load_distance / traffic_programme_para.empty_speed) / traffic_programme_para.payload
        # / traffic_programme_para.avg_goto_excavator_weight[load_area_index][unload_area_index]

        # 装载道路上，每提供1吨的装载能力需要一辆卡车运行时长,等于（该装载道路上车辆平均运行时长/卡车平均装载能力）
        # 单位为辆小时/吨
        # i代表第i个卸载点,j代表第j个挖机
        # walktime_goto_excavator单位是秒，需要除以3600，转成小时
        traffic_programme_para.goto_unload_area_factor[load_area_index][unload_area_index] = \
            (60 / 1000 * walk_time.to_unload_distance / traffic_programme_para.heavy_speed) / traffic_programme_para.payload
        # / traffic_programme_para.avg_goto_excavator_weight[unload_area_index][load_area_index]


# 从数据库中读取挖机和卸载点相关参数，并将线性规划所用参数保存在TrafficProgPara类中
def traffic_programme_para_init(num_of_load_area, num_of_unload_area, num_of_excavator, num_of_dump):
    # 初始化流量规划参数类
    traffic_programme_para = TrafficProgPara(num_of_load_area, num_of_unload_area, num_of_excavator, num_of_dump)

    extract_walk_time_info(traffic_programme_para)

    extract_excavator_info(traffic_programme_para)

    extract_dump_info(traffic_programme_para)

    # 全部矿卡设备集合
    truck_set = set(update_total_truck())

    # 固定派车矿卡集合
    fixed_truck_set = set(update_fixdisp_truck())

    # 动态派车矿卡集合
    traffic_programme_para.truck_total_num = len(truck_set.difference(fixed_truck_set))

    # 计算逻辑道路因子
    for i in range(num_of_excavator):
        for j in range(num_of_dump):
            # 查找挖机绑定的装载区, 卸载设备绑定的卸载区
            load_area_index = traffic_programme_para.excavator_index_to_load_area_index_dict[i]
            unload_area_index = traffic_programme_para.dump_index_to_unload_area_index_dict[j]

            # 逻辑道路因子赋值, 来自实际道路因子
            traffic_programme_para.goto_excavator_factor[j][i] = \
                traffic_programme_para.goto_load_area_factor[unload_area_index][load_area_index]

            traffic_programme_para.goto_unload_point_factor[i][j] = \
                traffic_programme_para.goto_unload_area_factor[load_area_index][unload_area_index]

            # 每条卸载道路的优先级,等于挖机的优先级乘以卸载点的优先级
            traffic_programme_para.priority_coefficient[i][j] = traffic_programme_para.excavator_priority_coefficient[i] \
                                                                * traffic_programme_para.dump_priority_coefficient[j]

            # 逻辑距离赋值，来自实际道路距离
            traffic_programme_para.goto_excavator_distance[j][i] = \
                traffic_programme_para.goto_load_area_distance[unload_area_index][load_area_index]

            traffic_programme_para.goto_dump_distance[i][j] = \
                traffic_programme_para.goto_unload_area_distance[load_area_index][unload_area_index]

    return traffic_programme_para


# 解决线性规划问题，生成每条道路的流量
def transportation_problem_slove(coefficient, w_ij, s_ij, b_excavator,
                                 b_dump, grade_loading_array,
                                 max_unload_weigh_alg_flag, truck_total_num,
                                 goto_excavator_dis, goto_dump_dis, min_throughout,
                                 grade_lower_array=None, grade_upper_array=None):
    row = len(coefficient)  # 代表挖机的个数,第i行代表第i台挖机
    col = len(coefficient[0])  # 代表卸载点的个数,第j行代表第j个卸载点

    # prob = pulp.LpProblem('Transportation Problem', sense=pulp.LpMaximize)
    # 卸载道路的流量,单位是吨/小时,i代表起点为第i个挖机,j代表终点为第j个卸载点
    var_x = [[pulp.LpVariable('x{0}{1}'.format(i, j), lowBound=0) for j in range(col)] for i in range(row)]
    # 装载道路的流量,单位是吨/小时,i代表起点为第i个卸载点,j代表终点为第j个挖机
    var_y = [[pulp.LpVariable('y{0}{1}'.format(i, j), lowBound=0) for j in range(row)] for i in range(col)]

    flatten = lambda x: [y for l in x for y in flatten(l)] if type(x) is list else [x]

    # 定义目标函数
    if max_unload_weigh_alg_flag == True:
        prob = pulp.LpProblem('Transportation Problem', sense=pulp.LpMaximize)
        # 得到目标函数，目标函数是使得系统的运输量最大
        prob += pulp.lpDot(flatten(var_x), coefficient.flatten())
    else:
        prob = pulp.LpProblem('Transportation Problem', sense=pulp.LpMinimize)
        goto_excavator_cost = var_x * goto_excavator_dis
        goto_dump_cost = var_y * goto_dump_dis
        prob += (pulp.lpSum(flatten(goto_excavator_cost)) + 1.5 * pulp.lpSum(flatten(goto_dump_cost)))

    # 定义约束条件
    # 最小产量约束，仅在最小化成本模式下成立
    if max_unload_weigh_alg_flag == False:
        prob += pulp.lpSum(var_x) >= min_throughout

    # 矿卡总数约束,在每条道路上的车辆总数要小于矿卡总个数
    # 通过矩阵按元素相乘得到每条卸载道路上的车辆个数
    unload_truck_total_num_array = w_ij * var_x
    # 通过矩阵按元素相乘得到每条装载道路上的车辆个数
    load_truck_totla_num_array = s_ij * var_y
    # 装载的矿卡数和卸载的矿卡数需要小于矿卡总数
    prob += (pulp.lpSum(unload_truck_total_num_array) +
             pulp.lpSum(load_truck_totla_num_array) <= truck_total_num)

    # 最大工作强度约束
    # 约束每个挖机的工作强度
    for i in range(row):
        prob += (pulp.lpSum(var_x[i]) <= b_excavator[i])
    # 约束每个卸载点的工作强度
    for j in range(col):
        prob += (pulp.lpSum(var_y[j]) <= b_dump[j])

    '''
    # 车流基尔霍夫定理约束
    # 进入挖机和从挖机出去的车辆个数需要相同
    for i in range(row):
        prob += (pulp.lpSum(unload_truck_total_num_array[i]) == pulp.lpSum(load_truck_totla_num_array[:,i]))
    # 从装载点离开和进来的车辆个数需要相同
    for j in range(col):
        prob += (pulp.lpSum(load_truck_totla_num_array[j]) == pulp.lpSum(unload_truck_total_num_array[:,j]))
    '''

    # 从装载点去往卸载点的流量之和要小于从卸载点到装载点的流量之和
    for i in range(row):
        prob += (pulp.lpSum((np.array(var_x))[i]) <= pulp.lpSum((np.array(var_y))[:, i]))

    # 从卸载点出发去往装载点的流量之和要小于从装载点到本卸载点的流量之和
    for j in range(col):
        prob += (pulp.lpSum((np.array(var_y))[j]) <= pulp.lpSum((np.array(var_x))[:, j]))

    # 矿石品位约束卸载
    # 去往卸载点的流量使用矩阵乘法乘以每个挖机处矿石的品位，得到每个卸载点的矿石品位总和
    grade_array = np.dot(grade_loading_array, var_x)
    for j in range(col):
        sum_traffic_unload = pulp.lpSum((np.array(var_x))[:, j])
        prob += (grade_array[j] >= sum_traffic_unload * grade_lower_array[j])
        prob += (grade_array[j] <= sum_traffic_unload * grade_upper_array[j])

    # 非负约束
    for i in range(row):
        for j in range(col):
            prob += var_x[i][j] >= 0
            prob += var_y[j][i] >= 0

    prob.solve()

    try:
        if -1 == prob.status:
            raise Exception("Model infeasible or unbounded")
    except Exception as es:
        logger.warning(es)

    return {'objective': pulp.value(prob.objective),
            'var_x': [[pulp.value(var_x[i][j]) for j in range(col)] for i in range(row)],
            'var_y': [[pulp.value(var_y[i][j]) for j in range(row)] for i in range(col)]}



def traffic_flow_plan():
    excavator_list = update_autodisp_excavator()

    dump_list = update_autodisp_dump()

    excavator_set = set(excavator_list)

    dump_set = set(dump_list)

    load_area_list = update_load_area()

    unload_area_list = update_unload_area()

    load_area_set = set(load_area_list)

    unload_area_set = set(unload_area_list)

    excavator_num = len(excavator_set)

    dump_num = len(dump_set)

    unload_area_num = len(unload_area_set)

    load_area_num = len(load_area_set)

    print("装载区数量:", load_area_num, "卸载区数量:", unload_area_num, "挖机数量:", excavator_num, "卸点数量:", dump_num)

    # 初始化参量
    traffic_programme_para = traffic_programme_para_init(load_area_num, unload_area_num, excavator_num, dump_num)

    # 系统是否以最大化产量为目标
    max_unload_weigh_alg_flag = False
    if max_unload_weigh_alg_flag:
        logger.info(f'最大产量调度模式')
    else:
        logger.info(f'最小成本调度模式')

    coefficient = traffic_programme_para.priority_coefficient
    w_ij = traffic_programme_para.goto_unload_point_factor
    s_ij = traffic_programme_para.goto_excavator_factor
    b_excavator = traffic_programme_para.excavator_strength
    b_dump = traffic_programme_para.dump_strength
    grade_loading_array = traffic_programme_para.grade_loading_array
    grade_lower_dump_array = traffic_programme_para.grade_lower_dump_array
    grade_upper_dump_array = traffic_programme_para.grade_upper_dump_array
    min_throughout = traffic_programme_para.min_throughout
    goto_excavator_distance = traffic_programme_para.goto_excavator_distance
    goto_dump_distance = traffic_programme_para.goto_dump_distance
    truck_total_num = traffic_programme_para.truck_total_num

    res = transportation_problem_slove(coefficient, w_ij, s_ij, b_excavator, b_dump,
                                       grade_loading_array, max_unload_weigh_alg_flag, truck_total_num,
                                       goto_excavator_distance, goto_dump_distance, min_throughout,
                                       grade_upper_dump_array, grade_lower_dump_array)

    if max_unload_weigh_alg_flag:
        print('最大化产量', res["objective"])
        logger.info(f'最大产量:{res["objective"]}')
    else:
        print('最小成本', res["objective"])
        logger.info(f'最小成本:{res["objective"]}')

    print('各变量的取值为：')
    logger.info('各变量取值:')
    print(np.array(res['var_x']).round(3))
    logger.info(f'重运车流:{res["var_x"]} 单位: 吨/时')
    print(np.array(res['var_y']).round(3))
    logger.info(f'空运车流:{res["var_y"]} 单位: 吨/时')

    # 通过矩阵按元素相乘得到每条卸载道路上的车辆个数
    unload_traffic = res['var_x']
    print((traffic_programme_para.goto_unload_point_factor * unload_traffic).round(3))
    # 通过矩阵按元素相乘得到每条装载道路上的车辆个数
    load_traffic = res['var_y']
    print((traffic_programme_para.goto_excavator_factor * load_traffic).round(3))

    return res["var_x"], res["var_y"]

traffic_flow_plan()

