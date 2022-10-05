"""
Deprecated. ibm_db has no support for pyinstaller_executables 
Please check the supported jaydebeapi connection details in below scripts for connection to db2 via python
https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/src/remote/python/connect_jaydebeapi.py
https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/src/remote/python/job_monitoring_functions_jaydebeapi.py
(or)
Most recent BDD framework tested 
https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/behave-testing/steps/python_scripts/jc_start.py
https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/behave-testing/steps/python_scripts/job_monitoring_functions.py

this program contains functions that adds new records to job_execution and job_step_execution db2 tables
"""

#from pyspark.sql import SparkSession, SQLContext, HiveContext
# call connect_to_db2.py function to establish db2 connection
#import jpype
#import jpype.imports

#from __future__ import absolute_import
#from ..temporary.connect import connect_db2

import sys, os
sys.path.append("..")

import numpy
import datetime
import ibm_db as db2
#import jaydebeapi as db2
from wrapper_scripts.connect import connect_db2
con = connect_db2()

#print(con)

# Implement getProcessId function
def get_process_id(application):	
	sql  = "select max(process_id) from JOB_EXECUTION where application_name = ? "
	st = db2.prepare(con,sql)

	db2.bind_param(st, 1, application.lower())
	db2.execute(st)
	final_id = db2.fetch_assoc(st)
	print("in GetProcessID final_id is ", final_id)
	return (final_id['1'])


def start_job_execution(tenant, application):
	process_id = get_process_id(application.lower())
	print ('process_id in start_job_execution: ', process_id)
	if process_id is None:
		process_id = 1
	else:
		process_id += 1;
	initialize_PID(process_id, tenant, application)
	return (process_id)

def initialize_PID(process_id, tenant, application):
	sysdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	print("initialized pid: ",process_id)
	sql =  "insert into JOB_EXECUTION (process_id, tenant_name, application_name, start_tst, end_tst, status) values (?, ?, ?, ?, NULL, 'started')"
	st = db2.prepare(con,sql)

	db2.bind_param(st, 1, process_id)
	db2.bind_param(st, 2, tenant.lower())
	db2.bind_param(st, 3, application.lower())
	db2.bind_param(st, 4, sysdate)
	db2.execute(st)

def set_status(process_id, status):
	sysdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	sql = "update JOB_EXECUTION set status = ?, end_tst = ? where process_id = ?"
	st = db2.prepare(con,sql)
	
	db2.bind_param(st, 1, status)
	db2.bind_param(st, 2, sysdate)
	db2.bind_param(st, 3, process_id)
	db2.execute(st)

def get_job_status(process_id):
	sql = "select status from JOB_EXECUTION where process_id = ?"
	st = db2.prepare(con,sql)
	
	db2.bind_param(st, 1, process_id)
	db2.execute(st)
	status = db2.fetch_assoc(st)
	print("in get_job_status: ", status)
	return (status['STATUS'])

def get_job_step_status(process_step_id):
	sql = "select step_status from JOB_STEP_EXECUTION where process_step_id = ?"
	st = db2.prepare(con,sql)
	
	db2.bind_param(st, 1, process_step_id)
	db2.execute(st)
	status = db2.fetch_assoc(st)
	print("in get_job_step_status: ", status)
	return (status['STEP_STATUS'])

def get_process_step_id():
	sql = "select max(process_step_id) from JOB_STEP_EXECUTION"
	st = db2.prepare(con, sql)
	db2.execute(st)
	process_step_id = db2.fetch_assoc(st)
	print("in getStepProcessID: ", process_step_id)
	return (process_step_id['1'])

def insert_job_step_execution(process_step_id, process_id, step_name):
	sysdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	sql = "insert into JOB_STEP_EXECUTION(process_step_id, process_id, step_name, step_start_tst, step_end_tst, step_status, read_count, filter_count, write_count, error_code, return_code) values(?, ?, ?, ?, NULL, 'started' ,NULL ,NULL ,NULL ,NULL ,1)"
	print("in insert_job_step_execution: ", process_step_id)
	st = db2.prepare(con, sql)

	db2.bind_param(st, 1, process_step_id)
	db2.bind_param(st, 2, process_id)
	db2.bind_param(st, 3, step_name)
	db2.bind_param(st, 4, sysdate)
	db2.execute(st)

def start_job_step_execution(process_id, step_name):
	process_step_id = get_process_step_id()
	if process_step_id is None or process_step_id is False:
		process_step_id = 1
	else:
		process_step_id += 1
	insert_job_step_execution(process_step_id, process_id, step_name)
	return (process_step_id)

def set_status_job_step_execution(process_step_id, status, error_code):
	sysdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	sql  = "update JOB_STEP_EXECUTION set step_status = ?, step_end_tst = ?, error_code =? where process_step_id = ?"
	st = db2.prepare(con, sql)

	db2.bind_param(st, 1, status)
	db2.bind_param(st, 2, sysdate)
	db2.bind_param(st, 3, error_code)
	db2.bind_param(st, 4, process_step_id)
	db2.execute(st)
	
def delete_row_job_step_execution(process_id):
	sql = "DELETE FROM JOB_STEP_EXECUTION WHERE process_id = ? ORDER BY PROCESS_STEP_ID DESC LIMIT 1"
	st = db2.prepare(con, sql)
	db2.bind_param(st, 1, process_id)
	db2.execute(st)

def set_return_code(process_step_id, return_code):
	sql = "update JOB_STEP_EXECUTION set return_code = ? where process_step_id = ?"
	st = db2.prepare(con, sql)
	db2.bind_param(st, 1, return_code)
	db2.bind_param(st, 2, process_step_id)
	db2.execute(st)

def set_read_count(process_step_id, read_count):
	sql = "update JOB_STEP_EXECUTION set read_count = ? where process_step_id = ?"
	st = db2.prepare(con, sql)
	db2.bind_param(st, 1, read_count)
	db2.bind_param(st, 2, process_step_id)
	db2.execute(st)

def set_filter_count(process_step_id, filter_count):
	sql = "update JOB_STEP_EXECUTION set filter_count = ? where process_step_id = ?"
	st = db2.prepare(con, sql)
	db2.bind_param(st, 1, filter_count)
	db2.bind_param(st, 2, process_step_id)
	db2.execute(st)

def set_write_count(process_step_id, write_count):
	sql = "update JOB_STEP_EXECUTION set write_count = ? where process_step_id = ?"
	st = db2.prepare(con, sql)
	db2.bind_param(st, 1, write_count)
	db2.bind_param(st, 2, process_step_id)
	db2.execute(st)

