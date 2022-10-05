from spark_cfg import spark_init
from job_monitoring_functions import *
from create_table_permissions import create_table_permissions
from load_core import load_rawvault
import traceback
import numpy
import datetime
import jaydebeapi as db2
import os

con = db2.connect('com.ibm.db2.jcc.DB2Driver', 'jdbc:db2://10.85.200.175:50000/db01', ['dbuser1', 'Xbv7J6A8RdqkP8mV'], './db2jcc4.jar')

curs = con.cursor()

def bearbnw_wrapper_rawvault():
	key_obj = {
		"table_name": "bearbnw",
		"tenant_name": "fw_test1",
		"application_name": "dwh",
		"local_loc" : "/shared/csv_files/bearbnw.csv",
		"hdfs_loc" : "/tmp/ingest/bddtest",
		"hdfs_file_loc" : "",
		"source_system" : "201",
		"source_database" : "rawvault_bddtest",
		"target_database" : "core_bddtest",
		"target_key_table" : "key_bearbnw",
		"partitions" : ["tenant_name", "process_id"],
		"primaryKeys": {},
		"foreignKeys": {}
        }

	key_obj['hdfs_file_loc']= key_obj['hdfs_loc'] + '/' + key_obj['local_loc'].split('/')[-1]

        #ingest
	spark = spark_init()
	pid = get_process_id(key_obj['application_name'], curs)
	psid = start_job_step_execution(pid, key_obj['table_name'].lower()+'_load_rawvault_'+key_obj['tenant_name']+'_'+key_obj['application_name'],curs)
	try:
		load_rawvault(spark, key_obj, pid, psid, curs)
	except:
		print(traceback.format_exc())
		set_status_job_step_execution(psid, key_obj['table_name'].lower()+'_load_rawvault_'+key_obj['tenant_name']+'_'+key_obj['application_name'], 'error', 1, curs)
	else:
		set_status_job_step_execution(psid, key_obj['table_name'].lower()+'_load_rawvault_'+key_obj['tenant_name']+'_'+key_obj['application_name'], 'finished', 0, curs)
		set_return_code(psid, 0, curs)

bearbnw_wrapper_rawvault()

curs.close()
con.close()
