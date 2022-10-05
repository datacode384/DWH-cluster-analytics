"""
this program contains functions that adds new records to job_execution and job_step_execution db2 tables
"""

#from pyspark.sql import SparkSession, SQLContext, HiveContext
# call connect_to_db2.py function to establish db2 connection
#import jpype
#import jpype.imports

import numpy
import datetime
#import ibm_db as db2
import jaydebeapi as db2
#from connect import connect_db2

# Implement getProcessId function
def get_process_id(application, curs):	
	print(application)
	sql  = "select max(process_id) from JOB_EXECUTION where application_name = ? "
	curs.execute(sql, [application])
	res =  curs.fetchall()
	final_id = res[0][0]
	print("in GetProcessID final_id is ", final_id)
	return (final_id)


def start_job_execution(tenant, application, curs):
	process_id = get_process_id(application, curs)
	print ('process_id in start_job_execution: ', process_id)
	if process_id is None:
		process_id = 1
	else:
		process_id += 1;
	initialize_PID(process_id, tenant, application,curs)
	return (process_id)

def initialize_PID(process_id, tenant, application, curs):
	sysdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	print(sysdate)
	sql =  "insert into JOB_EXECUTION (process_id, tenant_name, application_name, start_tst, end_tst, status) values (?, ?, ?, ?, ?, ?)"
	curs.execute(sql, [process_id,tenant,application,sysdate ,'NULL','started'])

def set_status(process_id, status, curs):
	sysdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	sql = "update JOB_EXECUTION set Status = ?, end_tst = ? where process_id = ?"
	curs.execute(sql, [status,sysdate,process_id])

def get_job_status(process_id, curs):
	sql = "select Status from JOB_EXECUTION where process_id = ?"
	curs.execute(sql,[process_id])
	#db2.bind_param(stmt, 1, process_id)
	print("in get_job_status: ", status)
	return (status)

def get_job_step_status(process_step_id, step_name,curs):
	sql = "select step_status from JOB_STEP_EXECUTION where process_step_id = ? and step_name = ?"
	curs.execute(sql,[process_step_id, step_name])
	res =  curs.fetchall()
	status = res[0][0]
	print("in get_job_step_status: ", status)
	return (status)

def get_process_step_id(curs):
	sql = "select max(process_step_id) from JOB_STEP_EXECUTION"
	curs.execute(sql)
	res =  curs.fetchall()
	print("res",res)
	process_step_id = res[0][0]
	print("in getStepProcessID: ", process_step_id)
	return (process_step_id)

def insert_job_step_execution(process_step_id, process_id, step_name, curs):
	sysdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	sql = "insert into JOB_STEP_EXECUTION(process_step_id, process_id, step_name, step_start_tst, step_end_tst, step_status, read_count, filter_count, write_count, error_code, return_code) values(?, ?, ?, ?, ?, ? ,? ,? ,? ,? ,?)"
	print("in insert_job_step_execution: ", process_step_id)	
	curs.execute(sql, [process_step_id, process_id, step_name, sysdate, 'NULL', 'started', 'NULL', 'NULL', 'NULL', 'NULL', 1])

def start_job_step_execution(process_id, step_name,curs):
	process_step_id = get_process_step_id(curs)
	if process_step_id is None or process_step_id is False:
		process_step_id = 1
	else:
		process_step_id += 1
	insert_job_step_execution(process_step_id, process_id, step_name, curs)
	return (process_step_id)

def set_status_job_step_execution(process_step_id, step_name, status, error_code,curs):
	sysdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	sql  = "update JOB_STEP_EXECUTION set step_status = ?, step_end_tst = ?, error_code =? where process_step_id = ? and step_name = ?"
	curs.execute(sql, [status,sysdate,error_code, process_step_id, step_name])
	
def delete_row_job_step_execution(process_id, curs):
	sql = "DELETE FROM JOB_STEP_EXECUTION WHERE process_id = ? ORDER BY PROCESS_STEP_ID DESC LIMIT 1"
	curs.execute(sql, [process_id])

def set_return_code(process_step_id, return_code, curs):
	sql = "update JOB_STEP_EXECUTION set return_code = ? where process_step_id = ?"
	curs.execute(sql, [return_code, process_step_id])

def set_read_count(process_step_id, read_count,curs):
	sql = "update JOB_STEP_EXECUTION set read_count = ? where process_step_id = ?"
	curs.execute(sql, [read_count, process_step_id])

def set_filter_count(process_step_id, filter_count,curs):
	sql = "update JOB_STEP_EXECUTION set filter_count = ? where process_step_id = ?"
	curs.execute(sql, [filter_count, process_step_id])

def set_write_count(process_step_id, write_count,curs):
	sql = "update JOB_STEP_EXECUTION set write_count = ? where process_step_id = ?"
	curs.execute(sql, [write_count, process_step_id])


