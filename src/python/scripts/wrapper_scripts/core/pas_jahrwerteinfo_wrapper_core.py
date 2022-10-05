import sys
sys.path.append("../..")
from scripts.wrapper_scripts.load_core import load_core
from monitoring.job_monitoring_functions import *
from scripts.wrapper_scripts.conf.spark_cfg import spark_init
from pyspark.sql import SparkSession
import traceback

def pas_jahrwerteinfo_wrapper_core():
        key_obj = {
                "table_name": "pas_jahrwerteinfo",
                "tenant_name": "ITERGO",
                "application_name": "DWH",
                "local_loc" : "/shared/original/pas_jahrwerteinfo.csv",
                "hdfs_loc" : "/tmp/ingests",
                "hdfs_file_loc" : "",
                "source_system" : "201",
                "source_database" : "raw_zone",
                "target_database" : "core",
                "target_key_table" : "key_pas_jahrwerteinfo",
                "partitions" : ["process_id"],
                "primaryKeys": { "id":{"field_name":"pas_jahrwerteinfo_id","fields":["lvid","vtid"]},
								"sid":{"field_name":"pas_jahrwerteinfo_sid","fields":["bearbidabg","lvid","process_id","vtid"]}
                },
                "foreignKeys": {"pas_vt_id":["lvid","vtid"]},
                "join_tables": [{"join_schema":"raw_zone","joined_table":"pas_bearbnw","join_columns":["bearbidabg","lvid"],"new_columns":[["wirkbeginn","wirksam_ab"],["bearbdat","gueltig_ab"]],"join_type":"inner"},{"join_schema":"core","joined_table":"pas_key_pas_lv","join_columns":["lvid"],"new_columns":[["tpa_mandant","tpa_mandant"]],"join_type":"left_outer"}]
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
	pas_jahrwerteinfo_wrapper_core()