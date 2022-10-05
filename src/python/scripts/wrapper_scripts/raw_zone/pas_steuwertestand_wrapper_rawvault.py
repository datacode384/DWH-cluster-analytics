import sys
sys.path.append("../..")
from scripts.wrapper_scripts.load_core import load_rawvault
from monitoring.job_monitoring_functions import *
from scripts.wrapper_scripts.conf.spark_cfg import spark_init
from pyspark.sql import SparkSession
import traceback

def pas_steuwertestand_wrapper_rawvault():
        key_obj = {
                "table_name": "pas_steuwertestand",
                "tenant_name": "ITERGO",
                "application_name": "DWH",
                "local_loc" : "/shared/original/pas_steuwertestand.csv",
                "hdfs_loc" : "/tmp/ingests",
                "hdfs_file_loc" : "",
                "source_system" : "201",
                "source_database" : "raw_zone",
                "target_database" : "core",
                "target_key_table" : "key_pas_steuwertestand",
                "partitions" : ["process_id"],
                "primaryKeys": {"id":{"field_name":"pas_steuschicht_id","fields":["lvid","schichtid","vtid"]}},
                "foreignKeys": {},
                "join_tables": []
        }
# add line
        key_obj['hdfs_file_loc']= key_obj['hdfs_loc'] + '/' + key_obj['local_loc'].split('/')[-1]

# rawvault
        spark = spark_init()
        pid = get_process_id(key_obj['application_name'])
        psid = start_job_step_execution(pid, key_obj['table_name'].lower()+'_load_rawvault_'+key_obj['tenant_name']+'_'+key_obj['application_name'])
        try:
                load_rawvault(spark, key_obj, pid, psid)
        except:
                print(traceback.format_exc())
                set_status_job_step_execution(psid, 'error', 1)
        else:
                set_status_job_step_execution(psid, 'finished', 0)
                set_return_code(psid, 0)
                
if __name__ == "__main__":
	pas_steuwertestand_wrapper_rawvault()