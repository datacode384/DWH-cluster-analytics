import sys
sys.path.append("../..")
from scripts.wrapper_scripts.load_core import load_core
from monitoring.job_monitoring_functions import *
from scripts.wrapper_scripts.conf.spark_cfg import spark_init
from pyspark.sql import SparkSession
import traceback

def pas_nbsmappingintext_wrapper_core():
        key_obj = {
                "table_name": "pas_nbsmappingintext",
                "tenant_name": "ITERGO",
                "application_name": "DWH",
                "local_loc" : "/shared/original/pas_nbsmappingintext.csv",
                "hdfs_loc" : "/tmp/ingests",
                "hdfs_file_loc" : "",
                "source_system" : "201",
                "source_database" : "raw_zone",
                "target_database" : "core",
                "target_key_table" : "key_pas_nbsmappingintext",
                "partitions" : ["process_id"],
                "primaryKeys": { "id":{"field_name":"pas_nbsmappingintext_id","fields":["id_int","id_type"]},
								"sid":{"field_name":"pas_nbsmappingintext_sid","fields":["id_int","id_type","process_id"]}
                },
                "foreignKeys": {},
                "join_tables": []
        }

        key_obj['hdfs_file_loc']= key_obj['hdfs_loc'] + '/' + key_obj['local_loc'].split('/')[-1]

# core
        spark = spark_init()
        pid = get_process_id(key_obj['application_name'])
        psid = start_job_step_execution(pid, key_obj['table_name'].lower()+'_load_core_'+key_obj['tenant_name']+'_'+key_obj['application_name'])
        try:
                load_core(spark, key_obj, pid, psid)
        except:
                print(traceback.format_exc())
                set_status_job_step_execution(psid, 'error', 1)
        else:
                set_status_job_step_execution(psid, 'finished', 0)
                set_return_code(psid, 0)
                
if __name__ == "__main__":
	pas_nbsmappingintext_wrapper_core()