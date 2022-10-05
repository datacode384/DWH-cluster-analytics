from spark_cfg import spark_init
from job_monitoring_functions import *
from create_table_permissions import create_table_permissions
from load_core import ingest
import traceback
import numpy
import datetime
import jaydebeapi as db2
import os

con = db2.connect('com.ibm.db2.jcc.DB2Driver', 'jdbc:db2://10.85.200.175:50000/db01', ['dbuser1', 'Xbv7J6A8RdqkP8mV'], './db2jcc4.jar')

curs = con.cursor()

def lv_wrapper_ingest():
    key_obj = {
        "table_name": "lv",
        "tenant_name": "fw_test1",
        "application_name": "dwh",
        "local_loc": "/shared/csv_files/lv.csv",
        "hdfs_loc": "/tmp/ingest/bddtest",
        "hdfs_file_loc": "",
        "source_system": "201",
        "source_database": "rawvault_bddtest",
        "target_database": "core_bddtest",
        "target_key_table": "key_lv",
        "partitions": ["tenant_name", "process_id"],
        "primaryKeys": {},
        "foreignKeys": {}
    }

    # uncomment the following and use it as the last argument when calling create_table_permissions when having different table_name in key_obj than original table name
    # file_name = 'lv'
    try:
        create_table_permissions(key_obj['source_database'], key_obj['target_database'], key_obj['table_name'].lower(),
                             key_obj['table_name'].lower())
    except:
        print(traceback.format_exc())

    key_obj['hdfs_file_loc'] = key_obj['hdfs_loc'] + '/' + key_obj['local_loc'].split('/')[-1]

    # ingest
    spark = spark_init()
    pid = get_process_id( key_obj['application_name'],curs)
    psid = start_job_step_execution(pid,
                                    key_obj['table_name'].lower() + '_ingest_' + key_obj['tenant_name'] + '_' + key_obj[
                                        'application_name'], curs)
    try:
        ingest(spark, key_obj, pid, psid)
    except:
        print(traceback.format_exc())
        set_status_job_step_execution(psid, key_obj['table_name'].lower()+'_ingest_'+key_obj['tenant_name']+'_'+key_obj['application_name'], 'error', 1, curs)
    else:
        set_status_job_step_execution(psid,
                                  key_obj['table_name'].lower() + '_ingest_' + key_obj['tenant_name'] + '_' + key_obj[
                                      'application_name'], 'finished', 0, curs)
        set_return_code(psid, 0, curs)

lv_wrapper_ingest()

curs.close()
con.close()
