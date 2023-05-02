#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: Backend
# datetime: 2018/11/8 12:03
# last modified by:
# software: PyCharm
from .dashboard import dashboard_transaction, dashboard_present, dashboard_exception,\
    dashboard_monitor, dashboard_online_device, dashboard_scheduled, dashboard_approval
from .dashboard_list import *
from mysite.base.tasks import get_employee_from_database_view