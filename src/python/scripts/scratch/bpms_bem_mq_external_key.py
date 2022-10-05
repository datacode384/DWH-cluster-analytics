from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql import Row 
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from load_core import load_rawvault
from job_monitoring_functions import *
import findspark
import re
import json
from spark_cfg import spark_init
import functools
from impala.dbapi import connect

def camel_to_snake(string):
	return re.sub(r'(?<!^)(?=[A-Z])', '_', string)
def parse_df(spark_obj, db, table, col, s_col, max_s_col):
	return spark_obj.sql("SELECT {0} FROM {1}.{2} WHERE {3}={4}".format(col, db, table, s_col, max_s_col))
def get_table_col_max(spark_obj, db, table, col):
	return spark_obj.sql("SELECT MAX({0}) FROM {1}.{2}".format(col, db, table)).collect()[0].asDict()['max('+col+')']
def check_key_obj(key_obj, message):
	# list(map(lambda x: camel_to_snake(x).upper(), asd))
	key_obj = set( eval(key_obj["message_filter"]))
	if type(message) == list:
		return key_obj.issubset( set(map(lambda x: camel_to_snake(x).upper(),functools.reduce(lambda a,b: set(a).union(b), message))) )
	elif type(message) == dict:
		return key_obj.issubset(set(map(lambda x: camel_to_snake(x).upper(), message)))
	else:
		return False
def process_test_mq_msg_filter():
	spark = spark_init()
	sc = spark.sparkContext
	key_obj = {"application_name": "DWH",
		"source_database" : "raw_zone",
		"source_table" : "bpms_bem_l0_dwh_in_1",
		"target_database" : "core",
		"target_table" : "bpms_bem_external_key",
		"target_key_table" : "key_bpms_bem_external_key",
		"tenant_name": "ITERGO",
		"source_system" : "201",
		"table_joins" : "@@@TABLE_JOINS@@@",
		"message_filter" : "{'ARCHIVE_DOCUMENT_ID', 'BUSINESS_EVENT_ID', 'DATE_INSERT', 'DATE_UPDATE', 'DWH_COUNTER', 'EXTERNAL_KEY_ID', 'EXTERNAL_KEY_TYPE', 'EXTERNAL_KEY_VALUE', 'GENERAL_DOCUMENT_ID', 'PACKAGE_ENTRY_ID', 'VERSION'}",
		"primaryKeys": {},
		"foreignKeys": {}
	}
	msg_max_tst = get_table_col_max(spark, key_obj['source_database'], key_obj['source_table'], "insert_tst")
	dwh_in_1 = parse_df(spark, key_obj['source_database'], key_obj['source_table'], "*", "insert_tst", msg_max_tst)
	parse_dicts = F.udf(lambda x: json.loads(x), MapType(StringType(), StringType()))
	parse_lists = F.udf(lambda x: json.loads(x) if x[0]=='[' else None, ArrayType(MapType(StringType(), StringType())))
	check_obj   = F.udf(lambda x: check_key_obj(key_obj,x), BooleanType())
	
	dwh_in_1 = dwh_in_1.withColumn("dict_msg", parse_dicts("message"))
	dwh_in_1 = dwh_in_1.withColumn("list_msg", parse_lists("message"))
	list_df  = dwh_in_1.select(explode(dwh_in_1.list_msg))
	dwh_in_1 = dwh_in_1.withColumn("dict_filter", check_obj("dict_msg"))
	list_df  = list_df.withColumn("list_filter", check_obj("col"))
	list_df  = list_df.filter(list_df['list_filter'] == True)
	dwh_in_1 = dwh_in_1.filter(dwh_in_1['dict_filter'] == True)
	final_col = 'dict_msg' if list_df.count() == 0 else 'list_msg'
	if final_col == 'list_msg':
		msg_list = list(map(lambda x: x.__getitem__("col"), list_df.select('col').collect()))
	else:
		msg_list = list(map(lambda x: x.__getitem__("dict_msg"), dwh_in_1.select('dict_msg').collect()))
	if msg_list == []:
		exit(1)
	final_df = sc.parallelize([msg_list[0]]).toDF() #create df for schema
	for msg in msg_list[1:]:
		final_df = final_df.union(sc.parallelize([msg]).toDF())
	for col_name in final_df.schema.names:
		final_df = final_df.withColumnRenamed(col_name, re.sub(r'(?<!^)(?=[A-Z])', '_', col_name).lower())
	final_df.registerTempTable("temp")
	spark.sql("insert into {0}.{1}  select * from temp".format(key_obj['target_database'],key_obj['table_name'].lower()))

if __name__ == "__main__":
	process_test_mq_msg_filter()
