import sys
sys.path.append("..")
from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql import Row 
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from wrapper_scripts.master_script_functions import *
from monitoring.job_monitoring_functions import *
import re
import functools
import findspark
import json


def get_datetime_now():
	now = datetime.datetime.now()
	current_time = now.strftime("%H:%M:%S")
	print("Current Time =", current_time)

def camel_to_snake(string):
	return re.sub(r'(?<!^)(?=[A-Z])', '_', string)
def snake_to_camel(string):
	string = string.split('_')
	return string[0].lower() + ''.join(x.title() for x in string[1:])

def get_max_value_records(spark_obj, db, table, col):
	max_value_col = spark_obj.sql("SELECT MAX({0}) FROM {1}.{2}".format(col, db, table)).collect()[0].asDict()['max('+col+')']
	return spark_obj.sql("SELECT * FROM {1}.{2} WHERE {3}={4}".format(db, table, col, max_value_col))
def get_first_x_records(spark_obj, key_obj, count):
	return spark_obj.sql("select * from {0}.{1}".format(key_obj["source_database"],key_obj["source_table"]))

def fill_missing_cols(current_df, template_df):
	for mcol in list(set(template_df.schema.names) - set(current_df.schema.names)):
		current_df = current_df.withColumn(mcol, lit(None).cast(template_df.schema[mcol].dataType))
	return current_df

def camel_to_snake_column_names(spark_df):
	for col_name in spark_df.schema.names:
		spark_df = spark_df.withColumnRenamed(col_name, re.sub(r'(?<!^)(?=[A-Z])', '_', col_name).lower())
	return spark_df

def add_technical_fields(spark, source, key_obj, psid):
	# join tables in order to get columns added to dataframe
	source = joins(spark, source, key_obj)
		
	# read count function call of records read from rawvault and joined (if applicable) with other tables
	set_read_count(psid, source.count())
	
	# create and adjjd hashes to dataframe
	source = hashes(spark, source, key_obj)
	
	core_df = spark.sql("select * from {0}.{1}".format(key_obj['target_database'],key_obj['target_table']))
	
	# adding foreign keys
	source = foreign_keys(spark, source, key_obj)

@udf(returnType = ArrayType(StringType()))
def get_converted_keys(dict_msg):
	try:
		keys = []
		for key_ in dict_msg:
			if key_ == "e2EProcessTimeslotStatus":
				key_ = "e2eProcessTimeslotStatus"
			keys.append(re.sub(r'(?<!^)(?=[A-Z])', '_', key_).upper())
		return keys
	except:
		return ["ERROR"]


@F.udf(returnType = BooleanType())
def compare_keys_filter(keys_col, filter_col):
	try:
		return set(filter_col).issubset(set(keys_col))
	except:
		return None

@F.udf(returnType = MapType(StringType(), StringType()))
def parse_dicts_new(message_column):
	try:
		return json.loads(message_column)
	except:
		return {"error":"error", "message":message_column}

def process_test_mq_msg_filter(spark, key_obj, psid, pid):
	spark.sparkContext.setLogLevel("ERROR")
	sc = spark.sparkContext
	get_datetime_now()
	print("### start load data ###")
	#dwh_in_1 = get_max_value_records(spark, key_obj['source_database'], key_obj['source_table'], "*", "insert_tst", msg_max_tst)
	dwh_in_1=spark.sql("select * from {0}.{1}".format(key_obj["source_database"],key_obj["source_table"])).limit(100)

	parse_dicts = udf(lambda x: json.loads(x), MapType(StringType(), StringType()))
	parse_lists = udf(lambda x: json.loads(x) if x[0]=='[' else None, ArrayType(MapType(StringType(), StringType())))
	dict_json_dumps = udf(lambda x: json.dumps(x), StringType())
	
	#dwh_in_1 = dwh_in_1.withColumn("dict_msg", parse_dicts("message"))
	dwh_in_1 = dwh_in_1.withColumn("dict_msg", parse_dicts_new("message"))
	dwh_in_1 = dwh_in_1.withColumn("list_msg", parse_lists("message"))
	dwh_in_1 = dwh_in_1.withColumn("dict_json_dumps", dict_json_dumps("dict_msg"))
	###
	print("before filtering list msgs for nulls: ", dwh_in_1.count())
	dwh_in_1_2 = dwh_in_1.filter(dwh_in_1.list_msg.isNotNull())
	print("after filtering list msgs for nulls:  ", dwh_in_1_2.count())
	
	# 
	# exploding list messages into new df and then merging it back to main df
	list_df  = dwh_in_1.select(explode(dwh_in_1_2.list_msg).alias("dict_msg1"), '*')
	list_df = list_df.drop(list_df.dict_msg)
	list_df = list_df.withColumnRenamed("dict_msg1", "dict_msg")
	list_df = list_df.select(dwh_in_1.schema.names)
	dwh_in_1 = dwh_in_1.union(list_df)
	dwh_in_1 = dwh_in_1.drop("list_msg")
	#


	# 
	# building columns for filtering
	dwh_in_1 = dwh_in_1.withColumn("src_filter", F.lit((key_obj["message_filter"])))
	print("before filtering main df for null dict messages: ", dwh_in_1.count())
	#dwh_in_1 = dwh_in_1.filter(dwh_in_1.dict_msg.isNotNull())
	print("after fltering main df for null dict messages: ", dwh_in_1.count())
	dwh_in_1 = dwh_in_1.withColumn("src_keys", get_converted_keys("dict_msg"))
	dwh_in_1 = dwh_in_1.withColumn('filter_fields', F.array([F.lit(x) for x in list(eval(key_obj['message_filter']))]))
	###
	dwh_in_1 = dwh_in_1.withColumn('bool_result', compare_keys_filter("src_keys", "filter_fields"))
	dwh_in_1 = dwh_in_1.filter(dwh_in_1.bool_result == True)

	# 
	# processing messages into dataframes
	final_df_template = spark.sql("SELECT * FROM {0}.{1}".format(key_obj["target_database"], key_obj["target_table"])).limit(0)
	json_fields_main = final_df_template.schema.names
	json_fields_main_camel = list(map(lambda x: snake_to_camel(x), json_fields_main ))
	json_fields_main_camel.remove("jmsMessageId")
	
	json_fields_filter = list(set(eval(key_obj["message_filter"])))
	struct_col = 'packageentries'
	dwh_in_1 = dwh_in_1.withColumnRenamed("jms_message_id", "jmsMessageId")
	df_tgt = dwh_in_1.select('jmsMessageId', F.json_tuple(dwh_in_1['dict_json_dumps'], *json_fields_main_camel ).alias(*json_fields_main_camel))
	insert_data(spark, df_tgt, pid, psid, key_obj)
	return df_tgt

def insert_data(spark, spark_df, pid, psid, key_obj):
	template = spark.sql("SELECT * FROM {0}.{1}".format(key_obj["target_database"], key_obj["target_table"])).limit(0)
	spark_df = fill_missing_cols(spark_df, template)
	spark_df = spark_df.select(template.schema.names)
	spark_df.registerTempTable("temp")
	test_df = spark.sql("select * from temp")
	test_df.show(100, False)
	spark.sql("insert into {0}.{1}  select * from temp".format(key_obj['target_database'],key_obj['target_table'].lower()))
	set_write_count(psid, spark.sql("select * from {0}.{1} where process_id = {2}".format(key_obj['target_database'], key_obj['target_table'].lower(), pid)).count())
