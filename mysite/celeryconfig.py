#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: Arvin
# datetime: 4/21/2020 12:15 PM
# software: BioTime
from django.conf import settings
from kombu import Queue
from celery.schedules import crontab


# Timezone setting
# timezone = tzlocal.get_localzone().zone
timezone = settings.TIME_ZONE
# enable_utc = True

# Broker Settings
broker_url = settings.BROKER_URL

# Task settings
task_always_eager = False
task_serializer = 'pickle'
task_ignore_result = True
accept_content = ['pickle', 'json', 'msgpack', 'yaml']

# Task result backend settings
result_backend = settings.CELERY_RESULT_BACKEND
result_serializer = 'json'
result_expires = 60 * 60
if not settings.DEBUG:
    task_ignore_result = True

# Logging
worker_log_format = "[%(asctime)s: %(levelname)s] %(message)s"
worker_task_log_format = "[%(asctime)s: %(levelname)s] [%(task_name)s] %(message)s"

# Queue
# if debug worker will run this task_queues tasks
if False:
    task_queues = (
        Queue('celery', routing_key='celery'),
        Queue('iclock_real_time', routing_key='iclock_real_time'),
        Queue('att_real_time', routing_key='att_real_time'),
        Queue('iclock_data_upload', routing_key='iclock_data_upload'),
        Queue('fast_run_queue', routing_key='fast_run_queue'),
        Queue('low_run_queue', routing_key='low_run_queue'),
    )

# # Routing
# task_routes = {}

# Worker
worker_max_tasks_per_child = 100
worker_concurrency = 3

# Beat Settings
DJANGO_CELERY_BEAT_TZ_AWARE = False
beat_scheduler = 'django_celery_beat.schedulers.DatabaseScheduler'


'''
定时任务统一配置, 不推荐使用装饰器配置定时任务
文档地址: http://docs.celeryproject.org/en/latest/userguide/periodic-tasks.html
periodic_task_name: 正常的命名
task_name: 装饰器@app.task(name='debug_task')种的name, 格式app_name.path.task_func_name
example:
    'periodic_task_name': {
        'task': 'task_name',
        'schedule': int or crontab,
        'args': list or tuple,
        'kwargs': dict,
    },

添加一个任务直接向app.conf.beat_schedule种添加即可
删除一个任务，或暂时关闭一个任务，需要相对应的删除数据库django_celery_beat_periodictask内的数据, 否则定时任务会继续运行
'''
beat_schedule = {
    # 'test_debug_task': {
    #     'task': 'debug_task',
    #     'schedule': 3,
    #     'args': [1, 2, 3],
    #     'kwargs': {'test': 'test', 'test1': 'test1'},
    # },
    'iclock.tasks.data_sync': {
        'task': 'iclock.tasks.data_sync',
        'schedule': 3,
    },
    'iclock.tasks.device_online_monitor': {
        'task': 'iclock.tasks.device_online_monitor',
        'schedule': settings.MAX_DEVICES_STATE,
    },

    'iclock.tasks.auto_data_compare': {
        'task': 'iclock.tasks.auto_data_compare',
        'schedule': crontab(minute=30, hour=22)
    },
    'mobile.task.upload_gps': {
        'task': 'mobile.task.upload_gps',
        'schedule': crontab(minute=30, hour=0),
    },
    'psnl.tasks.employment_status_monitoring': {
        'task': 'psnl.tasks.employment_status_monitoring',
        'schedule': crontab(minute=1, hour=0),
    },
    'psnl.tasks.resigned_scanner': {
        'task': 'psnl.tasks.resigned_scanner',
        'schedule': crontab(minute=5, hour=0),
    },
    'psnl.tasks.document_expired_alert': {
        'task': 'psnl.tasks.document_expired_alert',
        'schedule': crontab(minute=10, hour=0),
    },
    'meeting.tasks.meeting_monitor': {
        'task': 'meeting.tasks.meeting_monitor',
        'schedule': 60,
    },
    'att.tasks.restore_leaveyearbalance_all': {
        'task': 'att.tasks.restore_leaveyearbalance_all',
        'schedule': crontab(minute=20, hour=0, day_of_month=1, month_of_year=1),
    },
    'base.tasks.daily_licence_verify': {
        'task': 'base.tasks.daily_licence_verify',
        'schedule': crontab(minute=0, hour=2),
    },
    'base.tasks.daily_aof_rewrite': {
        'task': 'base.tasks.daily_aof_rewrite',
        'schedule': crontab(minute=30, hour=2),
    },
    'beat_tasks.run_minute_task': {
        'task': 'beat_tasks.run_minute_task',
        'schedule': 60,
    },
    'beat_tasks.get_employee_from_database_view':{
        'task':'base.tasks.get_employee_from_database_view',
        'schedule':60,
    },
    # 'beat_tasks.transaction_table_insert':{
    #     'task':'base.tasks.transaction_table_insert',
    #     'schedule':60,
    # },
}

task_routes = ([
    # iclock_real_time 命令任务生成很多可能被堵， 实时检测并下发给设备数据
    ('iclock.tasks.data_sync2device', {'queue': 'iclock_real_time'}),
    ('iclock.tasks.data_sync', {'queue': 'iclock_real_time'}),
    ('iclock.tasks.push_cmd2device', {'queue': 'iclock_real_time'}),

    # iclock_data_upload 设备上传，高峰期可能被堵，还待实际运行测试
    ('iclock.tasks.save_device_log', {'queue': 'iclock_data_upload'}),
    ('iclock.tasks.save_device_upload_log', {'queue': 'iclock_data_upload'}),
    ('iclock.tasks.save_device_capture', {'queue': 'iclock_data_upload'}),
    ('iclock.tasks.save_employee_photo', {'queue': 'iclock_data_upload'}),
    ('iclock.tasks.save_error_log', {'queue': 'iclock_data_upload'}),
    # ('iclock.tasks.update_command_result', {'queue': 'iclock_data_upload'}),
    # ('iclock.tasks.update_command_error_result', {'queue': 'iclock_data_upload'}),
    # ('iclock.tasks.update_terminal_parameter', {'queue': 'iclock_data_upload'}),
    ('mobile.tasks.save_clock_capture', {'queue': 'iclock_data_upload'}),

    # att_real_time 设备上传考勤记录相关，一次性上传很多考勤记录可能会堵
    ('iclock.tasks.real_time_new_transaction', {'queue': 'att_real_time'}),
    ('iclock.tasks.real_time_transaction', {'queue': 'att_real_time'}),
    ('att.tasks.run_calculation', {'queue': 'att_real_time'}),
    ('meeting.tasks.process_meeting_transaction', {'queue': 'att_real_time'}),
    ('visitor.tasks.process_visitor_transaction', {'queue': 'att_real_time'}),

    # fast_run_queue 非常紧急的队列，和用户交互的任务，不能堵塞影响用户体验
    ('base.tasks.async_progress_task_caller', {'queue': 'fast_run_queue'}),
    ('iclock.tasks.upload_all_data2device', {'queue': 'fast_run_queue'}),
    ('iclock.tasks.upload_employee', {'queue': 'fast_run_queue'}),
    ('iclock.tasks.transaction_correction', {'queue': 'fast_run_queue'}),
    ('iclock.tasks.transaction_correction_recheck', {'queue': 'fast_run_queue'}),
    ('iclock.tasks.get_terminal_file', {'queue': 'fast_run_queue'}),
    ('base.tasks.sync_adServer_data', {'queue': 'fast_run_queue'}),
    ('meeting.tasks.meeting_info_push', {'queue': 'fast_run_queue'}),
    ('psnl.tasks.remove_employee_form_area', {'queue': 'fast_run_queue'}),
    ('visitor.tasks.remove_visitor_from_area', {'queue': 'fast_run_queue'}),

    # low_run_queue 非常慢的任务，可能有大量的任务堵塞，运行速度慢，对运行时间不敏感
    ('base.tasks.asyn_send_one_mail', {'queue': 'low_run_queue'}),
    ('base.tasks.asyn_send_twilio_message', {'queue': 'low_run_queue'}),
    ('base.tasks.asyn_send_whatapp_message', {'queue': 'low_run_queue'}),
    ('mobile.tasks.push_mobile_msg', {'queue': 'low_run_queue'}),
    ('mobile.tasks.push_workflow_notification', {'queue': 'low_run_queue'}),
    ('mobile.tasks.process_public_announcement2notification', {'queue': 'low_run_queue'}),
    ('mobile.tasks.process_private_announcement2notification', {'queue': 'low_run_queue'}),
    ('mobile.tasks.process_approval2appiler', {'queue': 'low_run_queue'}),
    ('mobile.tasks.process_approval2approver', {'queue': 'low_run_queue'}),
    ('mobile.tasks.process_approval2notifier', {'queue': 'low_run_queue'}),

    # celery 默认的队列，一般任务，正常不会被堵塞，包括每日运行任务，每分钟检测任务，其他未设置任务
    ('iclock.tasks.device_online_monitor', {'queue': 'celery'}),
    ('base.tasks.daily_licence_verify', {'queue': 'celery'}),
    ('meeting.tasks.meeting_monitor', {'queue': 'celery'}),
    ('psnl.tasks.employment_status_monitoring', {'queue': 'celery'}),
    ('psnl.tasks.resigned_scanner', {'queue': 'celery'}),
    ('psnl.tasks.document_expired_alert', {'queue': 'celery'}),
    ('mobile.task.upload_gps', {'queue': 'celery'}),
    ('mobile.tasks.update_personnel_gps', {'queue': 'celery'}),
    ('mobile.tasks.update_employee_gps', {'queue': 'celery'}),
    ('iclock.tasks.employee_filter', {'queue': 'celery'}),
    ('iclock.tasks.daily_aof_rewrite', {'queue': 'celery'}),

    ('base.beat_tasks.DatabaseBackupTask', {'queue': 'celery'}),
    ('base.beat_tasks.MiddlewareTableMigrateTask', {'queue': 'celery'}),
    ('base.beat_tasks.LdapSyncTask', {'queue': 'celery'}),
    ('iclock.beat_tasks.TransactionFromFile2DbTask', {'queue': 'celery'}),
    ('base.beat_tasks.TransactionAutoExport', {'queue': 'celery'}),
    ('base.beat_tasks.AttReportAutoExport', {'queue': 'celery'}),
    ('att.beat_tasks.AttExceptionAlertTask', {'queue': 'celery'}),
    ('meeting.beat_tasks.MeetingCalcTask', {'queue': 'celery'}),
    ('visitor.beat_tasks.ExitScannerTask', {'queue': 'celery'}),
    ('base.tasks.log_recorder', {'queue': 'celery'}),
    ('base.tasks.get_employee_from_database_view',{'queue':'celery'}),
    # ('base.tasks.transaction_table_insert',{'queue':'celery'}),
],)
