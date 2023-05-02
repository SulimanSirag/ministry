import datetime
import json
import logging
import os

from celery.utils.log import get_task_logger
from celery.contrib.abortable import AbortableTask
from django.core.cache import cache
from importlib import import_module
from django.core.exceptions import ValidationError
from django.conf import settings
from twilio.base.exceptions import TwilioRestException
from django.utils.translation import activate
from mysite.base.models import EmailTemplate
from mysite.base import email_const

from mysite.base import const
from mysite.base.utils import save_system_log
from mysite import celery_app
from mysite.admin.services.email import send_one_mail_with_attachments
from mysite.tools.celery_utils import progress_recorder_adder
from mysite.tools.twilio_utils import send_twilio_message, send_whatapp_message
from mysite.base.models import SystemSetting
from mysite.admin.helpers import time_it
from . import beat_tasks

logger = get_task_logger(__name__)


@celery_app.task(name='base.tasks.asyn_send_one_mail')
def asyn_send_one_mail(subject, body, to, context=None, to_cc=None, to_bcc=None,
                       subject_template_name=None, content_template_name=None,
                       attachments=None):
    try:
        es = SystemSetting.cache_data(name='email_setting')
        if not (es and json.loads(es.value).get('email_enable')):
            return

        to = [i for i in to if i]
        if not to:
            return 'To is None'

        send_one_mail_with_attachments(subject, body, to, context=context, to_cc=to_cc, to_bcc=to_bcc,
                                       subject_template_name=subject_template_name,
                                       content_template_name=content_template_name,
                                       attachments=attachments)
    except Exception:
        logging.getLogger('task').exception(
            'asyn_send_one_mail: %s --> %s' % (to, subject))


@celery_app.task(name='base.tasks.asyn_send_twilio_message')
def asyn_send_twilio_message(body, to, from_=None, client=None):
    try:
        if not to:
            return 'To is None'
        send_twilio_message(body, to, from_=from_, client=client)
    except TwilioRestException as e:
        logging.getLogger('task').exception(
            'asyn_send_one_mail: %s --> %s, error:%s' % (to, body, e.msg))


@celery_app.task(name='base.tasks.asyn_send_whatapp_message')
def asyn_send_whatapp_message(body, to, from_=None, client=None):
    try:
        if not to:
            return 'To is None'
        # send_whatapp_message(body, to, from_=from_, client=client)
    except TwilioRestException as e:
        logging.getLogger('task').exception(
            'asyn_send_one_mail: %s --> %s, error:%s' % (to, body, e.msg))


@celery_app.task(name='base.tasks.daily_aof_rewrite')
def daily_aof_rewrite(force_rewrite=False):
    from mysite.tools.redis_utils import celery_worker_redis
    aof_path = os.path.join(settings.BASE_DIR, 'redis', 'appendonly.aof')
    rdb_path = os.path.join(settings.BASE_DIR, 'redis', 'dump.rdb')
    aof_size = 0
    rdb_size = 0
    if os.path.exists(aof_path):
        aof_size = os.path.getsize(aof_path) // 1000
    if os.path.exists(rdb_path):
        rdb_size = os.path.getsize(rdb_path) // 1000

    logger.info('aof size: %s' % aof_size)
    logger.info('rdb size: %s' % rdb_size)

    # 当aof比rdb大 100M 时重写aof
    if force_rewrite or (aof_size - rdb_size) > 100 * 1000:
        logger.info('bgrewriteaof: %s' % datetime.datetime.now())
        celery_worker_redis.client.bgrewriteaof()
    else:
        logger.info('no need rewriteaof: %s' % datetime.datetime.now())

    # celery_worker_redis.client.bgrewriteaof()


@celery_app.task(name='base.tasks.daily_licence_verify')
def daily_licence_verify():
    from django.conf import settings
    from mysite.core.zkmimi import getISVALIDDONGLE
    from mysite.base.license import license_online_verify
    try:
        cache.delete('{unit}-zklicense'.format(unit=settings.UNIT))
        ret = getISVALIDDONGLE(reload=1)
        license_online_verify(ret[1])
        # save_system_log(const.LICENSE_VERIFY, const.SUCCESS, str(msg))
    except Exception as e:
        # save_system_log(const.LICENSE_VERIFY, const.FAILED, str(e))
        logging.getLogger('license').exception('daily_licence_verify')


@celery_app.task(name='base.tasks.log_recorder')
def log_recorder(user, ip, can_routable, objects, action, status=0, messages="", model=None):
    from itertools import filterfalse
    from django.utils import six
    from django.utils.translation import override as translation_override
    from django.contrib.admin.options import get_content_type_for_model
    from mysite.base.models import AdminLog
    from mysite.base.db_const import MAX_ACTION_NAME
    from mysite.admin import zk_site

    if isinstance(model, six.string_types):
        model = zk_site.get_model_admin(model)

    if isinstance(messages, (list, tuple, dict)):
        messages = json.dumps(messages)
    elif isinstance(messages, six.string_types):
        pass
    else:
        raise TypeError(
            "messages is either one of string types or json-encode-able format.")

    if callable(action):
        # deactivate all other language temporarily
        with translation_override(None):
            action_name = action()
    elif isinstance(action, six.string_types):
        action_name = action[:MAX_ACTION_NAME]
    else:
        raise TypeError(
            "action_name is either one of string types or non-argument callable object")

    def _handle_pk(objects, i, pk):
        if model is None:
            raise ValueError(
                'If obj is integer key, then Caller must provide model_class')

        objects[i] = model.objects.filter(pk=pk).first()

    if isinstance(objects, six.string_types) and not objects.isdigit():
        objects = objects.replace("|", "")
        targets = ""
        targets_repr = objects
        objects = []
    else:
        if not isinstance(objects, (tuple, list)):
            objects = [objects, ]

        for i, obj in enumerate(objects):
            if isinstance(obj, six.integer_types):
                _handle_pk(objects, i, obj)
            elif isinstance(obj, six.string_types) and obj.isdigit():
                _handle_pk(objects, i, int(obj))

        objects = list(filterfalse(lambda x: x is None, objects))
        targets = json.dumps([getattr(obj, 'pk', '') for obj in objects])
        targets_repr = ','.join(["{obj}".format(obj=obj) for obj in objects])

    log = AdminLog(
        user=user, action=action_name, ip_address=ip, can_routable=can_routable,
        content_type_id=get_content_type_for_model(
            objects[0] if len(objects) else model).pk,
        description=messages, targets=targets, targets_repr=targets_repr, action_status=status)
    try:
        log.save()
    except Exception:
        logging.getLogger('task').exception('log_recorder error')


def async_progress_task_wapper(call_info, *args, **kwargs):
    delay = True
    if 'delay' in call_info and call_info['delay'] is False:
        delay = False
    else:
        if settings.ASYN_PROGRESS_TASK_SYNC:
            delay = False
    if delay is True:
        return async_progress_task_caller.delay(call_info, *args, **kwargs)
    else:
        return async_progress_task_caller(call_info, *args, **kwargs)


@celery_app.task(bind=True, name='base.tasks.async_progress_task_caller', ignore_result=False, base=AbortableTask)
@progress_recorder_adder
@time_it
def async_progress_task_caller(task, call_info, *args, **kwargs):
    lng = call_info.get('lng')
    if lng:
        activate(lng)
    func = None
    call_type = call_info.get('type')
    if call_type == 'admin':
        from mysite.admin import zk_site
        model = call_info.get('model')
        func = call_info.get('func')
        admin = zk_site.get_model_admin(model)
        func = getattr(admin, func, None)
    elif call_type == 'apiview':
        module = import_module(call_info.get('model'))
        viewset = getattr(module, call_info.get('class'))()
        viewset.request = kwargs['request']
        viewset.action = call_info.get('action')
        viewset.initial(kwargs['request'])
        func = getattr(viewset, call_info.get('func'), None)
    elif call_type == 'func':
        func = call_info.get('func')

    assert callable(func), 'type %s: %s is not callable.' % (
        type(func), str(func))
    return func(*args, **kwargs)


@celery_app.task(name='bask.tasks.sync_adServer_data')
def sync_ad_server_data():
    from mysite.tools.ldap_utils import LDAPClient
    with LDAPClient() as _ldap:
        try:
            _ldap.bind()
            _ldap.sync_employee()
            save_system_log(const.LDAP_SYNC, const.SUCCESS, '')
        except Exception:
            save_system_log(const.LDAP_SYNC, const.FAILED, '')
            logging.getLogger('task').exception('sync_ad_server_data')
@celery_app.task(name='base.tasks.delivery_exception_email_to_superuser')
def delivery_exception_email_to_superuser(manager, items, start_date, end_date):
    import os
    from mysite.utils import tempDir
    from mysite.admin.services.email import send_one_mail_with_attachments

    print(('email:', manager.email))
    if manager.email:
        to = [manager.email]
        header = ['Name', 'Date', 'TimeTable', 'Check-In', 'Check-Out', 'Clock-In', 'Clock-Out', 'Absent']
        cells = []
        for item in items:
            data = item['data']
            name = item['emp__first_name']
            for d in data:
                absent = ''
                late_in = early_out = None
                att_date = d['att_date'].strftime("%m-%d-%Y")
                timetable = d['time_card__time_table_alias']
                checkin = d['time_card__check_in'].strftime("%H:%M:%S")
                checkout = d['time_card__check_out'].strftime("%H:%M:%S")
                if d['absence'] == 1:
                    absent = 'Absent'
                if d['late_in_punch'] is not None:
                    late_in = d['late_in_punch'].strftime("%H:%M:%S")
                if d['early_out_punch'] is not None:
                    early_out = d['early_out_punch'].strftime("%H:%M:%S")
                cell = (name, att_date, timetable, checkin, checkout, late_in, early_out, absent)
                cells.append(cell)
            cell = ('', '','','','', str(item['late_in']), str(item['early_out']),
                    str(item['absence']))
            cells.append(cell)

        save_path = os.path.join(tempDir(), 'attendance_exception')
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        file_name = 'AttendanceExceptionDept_' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        file_path = os.path.join(save_path, '{file}.{format}'.format(file=file_name, format='xls'))
        excel_builder(header, cells, file_path)

        et = EmailTemplate.objects.filter(event=email_const.EXCEPTION_EVENT,
                                          receiver=email_const.RECEIVER_ADMIN).first()
        subject = 'Attendance Exception'
        template = et.email_content
        body = template.format(manager=manager.username,
                               start_date=start_date.strftime('%Y-%m-%d %H:%M:%S'),
                               end_date=end_date.strftime('%Y-%m-%d %H:%M:%S'))
        send_one_mail_with_attachments(subject, body, to, attachments=[file_path, ])


def excel_builder(headers, data_list, file_path):
    import xlwt
    wb = xlwt.Workbook(encoding=u"utf-8")
    ws = wb.add_sheet('Attendance Exception')
    fnt = xlwt.Font()
    fnt.name = 'Arial'

    borders = xlwt.Borders()
    borders.left = 1
    borders.right = 1
    borders.top = 1
    borders.bottom = 1

    align = xlwt.Alignment()
    align.horz = xlwt.Alignment.HORZ_CENTER
    align.vert = xlwt.Alignment.VERT_CENTER

    style = xlwt.XFStyle()
    style.font = fnt
    style.borders = borders
    style.alignment = align

    data_list.insert(0, headers)
    wcells = [[max(len(item if item else ''), 6) + 1 for item in line]
              for i, line in enumerate(data_list)]
    wcs = list(map(max, zip(*wcells)))
    for row_index, row in enumerate(data_list):
        for col_index, col in enumerate(row):
            ws.write(row_index, col_index, col, style)
            ws.col(col_index).width = wcs[col_index] * 256

    wb.save(file_path)
    print('>>>export success path:', file_path)


@celery_app.task(name='base.tasks.delivery_exception_email_to_depament_manage')
def delivery_exception_email_to_depament_manage(manager, items, start_date, end_date):
    """
    Delivery email after attendance calculation(it depends on alert setting)
    :param dept: Department
    :param items: [{'emp_code':'***', late_times': 0, 'early_leave_times': 0, 'absent_times': 0 ...}]
    :param start_date: Calculation Start Date
    :param end_date: Calculation End Date
    :return:
    """
    import os
    from mysite.utils import tempDir
    from mysite.admin.services.email import send_one_mail_with_attachments

    print(('email:', manager.email))
    if manager.email:
        to = [manager.email]
        header = ['Name', 'Date', 'TimeTable', 'Check-In', 'Check-Out', 'Clock-In', 'Clock-Out', 'Absent']
        cells = []
        for item in items:
            data = item['data']
            name = item['emp__first_name']
            for d in data:
                absent = ''
                late_in = early_out = None
                att_date = d['att_date'].strftime("%m-%d-%Y")
                timetable = d['time_card__time_table_alias']
                checkin = d['time_card__check_in'].strftime("%H:%M:%S")
                checkout = d['time_card__check_out'].strftime("%H:%M:%S")
                if d['absence'] == 1:
                    absent = 'Absent'
                if d['late_in_punch'] is not None:
                    late_in = d['late_in_punch'].strftime("%H:%M:%S")
                if d['early_out_punch'] is not None:
                    early_out = d['early_out_punch'].strftime("%H:%M:%S")
                cell = (name, att_date, timetable, checkin, checkout, late_in, early_out, absent)
                cells.append(cell)
            cell = ('', '','','','', str(item['late_in']), str(item['early_out']),
                    str(item['absence']))
            cells.append(cell)

        save_path = os.path.join(tempDir(), 'attendance_exception')
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        file_name = 'AttendanceExceptionAdmin_' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        file_path = os.path.join(save_path, '{file}.{format}'.format(file=file_name, format='xls'))
        excel_builder(header, cells, file_path)

        et = EmailTemplate.objects.filter(event=email_const.EXCEPTION_EVENT,
                                          receiver=email_const.RECEIVER_ADMIN).first()
        subject = 'Attendance Exception'
        template = et.template
        body = template.format(manager=manager.username,
                               start_date=start_date.strftime('%Y-%m-%d %H:%M:%S'),
                               end_date=end_date.strftime('%Y-%m-%d %H:%M:%S'))
        send_one_mail_with_attachments(subject, body, to, attachments=[file_path, ])


gender_dict = {
    'ذكر': 'M', 'أنثى': 'F'
}

EMP_VIEW_SQL="""
SELECT EMP_CODE,NATIONAL_NO,EMPLOYEE_FIRST_NAME_AR,EMPLOYEE_FAMILY_NAME_AR,JOB_ORG_ID,JOB_ORG_NAME,JOB_NO,ACTUAL_JOB_NAME,
    CENTER_NO,CENTER_NAME,CENTER_HIRE_DATE_GRG,BIRTH_DATE_GRG,GENDER,EMAIL,MOBILE
    FROM WEQAA_HR_INT_EMPLOYEE_INFO WHERE FLAG='FALSE'
"""


     
def emp_view_update(ids):
    # return " UPDATE WEQAA_HR_INT_EMPLOYEE_INFO SET FLAG='TRUE' WHERE EMP_CODE in ({})".format(','.join(ids))
    return f" UPDATE WEQAA_HR_INT_EMPLOYEE_INFO SET FLAG='TRUE' WHERE EMP_CODE in ({','.join(ids)})"


@celery_app.task(name="base.tasks.get_employee_from_database_view")
def get_employee_from_database_view():
    from mysite.personnel.models import Employee , Resign
    from mysite.admin.models import STATUS_VALID,STATUS_INVALID,STATUS_RESIGN
    from mysite.base.views.middletable_migrations import department_valid,area_valid,position_valid
    from mysite.personnel.utils import get_default_area,get_default_department
    from mysite.sql_utils import p_query,p_execute
    from mysite.base.models.middleware_tables import SyncEmployee
    raw_data=p_query(EMP_VIEW_SQL)
    print("_++++++++++++++++++++ View data_list ++++++++++++++++_",raw_data)
    emp_codes=[]
    if raw_data:
        for r in raw_data:
            try:
                info_dict={}
                emp_code=str(r[0])
                # national=r[1]
                department_code=r[4]
                department_name=r[5]
                area_code=str(int(r[8]))
                area_name=r[9]
                position_code=r[6]
                position_name=r[7]
                department=department_valid(department_code,department_name)
                area=area_valid(area_code,area_name)
                position=position_valid(position_code,position_name)
                emp_poition=position or None
                emp_department=department or get_default_department()
                emp_area=area or get_default_area()
                info_dict['emp_code']=emp_code
                info_dict['national']=r[1]
                info_dict['first_name']=r[2]
                info_dict['last_name']=r[3]
                info_dict['department']=emp_department
                info_dict['position']=emp_poition
                info_dict['hire_date']=r[10]
                info_dict['gender']=gender_dict.get(r[12],'M')
                info_dict['birthday']=r[11]
                info_dict['email']=r[13]
                info_dict['mobile']=r[14]
                emp,created=Employee.objects.update_or_create(emp_code=emp_code,defaults=info_dict)
                emp.area=[emp_area]
                emp_codes.append(emp.emp_code)
            except Exception as e:
                print("______________some Error ______________",e)
        if emp_codes:
            raw_data=p_execute(emp_view_update(emp_codes))
    return True

# get_emp=get_employee_from_database_view()
# print("++++++++++++++get employee views +++++++++++",get_emp)

