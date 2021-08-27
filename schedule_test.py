#!E:\Pycharm Projects\Waytous
# -*- coding: utf-8 -*-
# @Time : 2021/8/24 14:56
# @Author : Opfer
# @Site :
# @File : schedule_test.py    
# @Software: PyCharm

import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

def my_job(id='my_job'):
    print(id, '-->', datetime.datetime.now())

jobstores = {'default': MemoryJobStore()}

executors = {'default': ThreadPoolExecutor(10)}

job_defaults = {'coalesce': False, 'max_instances': 10}

scheduler = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)

scheduler.add_job(my_job, args=['job_interval'], id='ins1', trigger='interval', seconds=5)

scheduler.add_job(my_job, args=['job_interval'], id='ins2', trigger='interval', seconds=5)

if __name__ == '__main__':
    try:
        scheduler.start()
    except SystemExit:
        print('exit')
        exit()