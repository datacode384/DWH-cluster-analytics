import sys
sys.path.append("..")
from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql import Row 
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from monitoring.job_monitoring_functions import *
from wrapper_scripts.conf.spark_cfg import spark_init
from wrapper_scripts.bpms_bem_utils import process_test_mq_msg_filter
import functools
import traceback

def load_bpms_bem_complaint():
	key_obj = {"application_name": "DWH",
		"source_database" : "raw_zone",
		"source_table" : "bpms_bem_l0_dwh_in_1",
		"target_database" : "core",
		"target_table" : "bpms_bem_complaint",
		"target_key_table" : "key_bpms_bem_complaint",
		"tenant_name": "@@@TENANT@@@",
		"source_system" : "201",
		"join_tables" : [],
		"message_filter" : "{'COMPLAINT_ID', 'COMPLAINT_TYPE', 'COMPLAINT_VARIANT_TYPE', 'COMPLAINT_STATUS', 'COMPLAINT_TOP_FLAG', 'COMPLAINT_INIT_TIMESTAMP', 'COMPLAINT_OWNER_TYPE', 'COMPLAINT_INITIATOR_TYPE', 'PROCESS_GROUP_HCM'}",
		"primaryKeys": { "id":{"field_name":"bpms_bem_complaint_id","fields":["complaint_id"]}, "sid":{"field_name":"bpms_bem_complaint_sid","fields":["complaint_id","jms_message_id","process_id"]} },
		"foreignKeys": { "bpms_bem_business_event_id":["business_event_id"] }
	}
	spark = spark_init()
	pid = get_process_id(key_obj['application_name'])
	psid = start_job_step_execution(pid, key_obj['target_table'].lower()+'_load_core_'+key_obj['tenant_name']+'_'+key_obj['application_name'])
	try:
		temp_df = process_test_mq_msg_filter(spark, key_obj, psid)
		#temp_df.show()
		#insert_data(spark, process_test_mq_msg_filter(spark, key_obj, psid), pid, psid, key_obj)
	except:
		print(traceback.format_exc())
		set_status_job_step_execution(psid, 'error', 1)
	else:
		set_status_job_step_execution(psid, 'finished', 0)
		set_return_code(psid, 0)

if __name__ == "__main__":
	load_bpms_bem_complaint()
