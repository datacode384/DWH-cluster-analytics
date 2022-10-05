import sys
sys.path.append("..")
from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import wrapper_scripts.master_script_functions
import re
import functools
import findspark
import json


def get_datetime_now():
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("Current Time =", current_time)

def camel_to_snake(string):
        return re.sub(r'(?<!^)(?=[A-Z])', '_', string)

def get_max_value_records(spark_obj, db, table, col):
        max_value_col = spark_obj.sql("SELECT MAX({0}) FROM {1}.{2}".format(col, db, table)).collect()[0].asDict()['max('+col+')']
        return spark_obj.sql("SELECT * FROM {1}.{2} WHERE {3}={4}".format(db, table, col, max_value_col))
def get_first_x_records(spark_obj, key_obj, count):
        return spark_obj.sql("select * from {0}.{1}".format(key_obj["source_database"],key_obj["source_table"]))

def fill_missing_cols(current_df, template_df):
        for mcol in list(set(template_df.columns) - set(current_df.columns)):
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
        source = hashes(spark, source, key_obj.py

        core_df = spark.sql("select * from {0}.{1}".format(key_obj['target_database'],key_obj['target_table']))
			
# adding foreign keys
        source = foreign_keys(spark, source, key_obj)

@udf(returnType = ArrayType(StringType()))
def get_converted_keys(dict_msg):
        try:
                list_key = list(dict_msg.keys())
                keys = []
                for key_ in list_key:
                        keys.append(re.sub(r'(?<!^)(?=[A-Z])', '_', key_).upper())
                return keys
        except:
                return ["error_error"]


@F.udf(returnType=BooleanType())
def my_filter(col1, col2):
        return set(eval(col1)).issubset(set(col2))

def process_test_mq_msg_filter(spark, key_obj, psid):
        sc = spark.sparkContext

        print("### start load data ###")
        get_datetime_now()
        #dwh_in_1 = get_max_value_records(spark, key_obj['source_database'], key_obj['source_table'], "*", "insert_tst", msg_max_tst)
        dwh_in_1=spark.sql("select * from {0}.{1}".format(key_obj["source_database"],key_obj["source_table"]))
        parse_dicts = udf(lambda x: json.loads(x), MapType(StringType(), StringType()))
        parse_lists = udf(lambda x: json.loads(x) if x[0]=='[' else None, ArrayType(MapType(StringType(), StringType())))
        #parse_keys  = udf( list(set(list(lambda x: re.sub(r'(?<!^)(?=[A-Z])', '_', x).upper())) , ArrayType( StringType() ))
        get_msg_key_dict = udf( lambda x: set(eval(key_obj["message_filter"])).issubset( set(map(lambda y: re.sub(r'(?<!^)(?=[A-Z])', '_', y).upper(), x)) ) , BooleanType() )

        print("### start filter  ###")
        get_datetime_now()

        dwh_in_1 = dwh_in_1.withColumn("dict_msg", parse_dicts("message"))
        dwh_in_1 = dwh_in_1.withColumn("list_msg", parse_lists("message"))
        list_df  = dwh_in_1.select(explode(dwh_in_1.list_msg).alias("dict_msg1"), '*')
        list_df = list_df.drop(list_df.dict_msg)
        list_df = list_df.withColumnRenamed("dict_msg1", "dict_msg")
        list_df = list_df.select(dwh_in_1.schema.names)
        dwh_in_1 = dwh_in_1.union(list_df)
        dwh_in_1 = dwh_in_1.drop("list_msg", "message")
        dwh_in_1 = dwh_in_1.withColumn("src_filter", F.lit((key_obj["message_filter"])))
        dwh_in_1 = dwh_in_1.filter(dwh_in_1.dict_msg.isNotNull())
        dwh_in_1 = dwh_in_1.withColumn("src_keys", get_converted_keys("dict_msg"))
        dwh_in_1 = dwh_in_1.withColumn("filter_condition", my_filter("src_filter", "src_keys"))
        dwh_in_1 = dwh_in_1.withColumn('filter_fields', F.array([F.lit(x) for x in list(eval(key_obj['message_filter']))]))
        dwh_in_1 = dwh_in_1.withColumn('filter_result',  array_except('filter_fields','src_keys'))
        dwh_in_1 = dwh_in_1.withColumn('bool_result', F.size(F.col('filter_result'))==0)
			
	dwh_in_1.show()
        print(dwh_in_1.count())
        dwh_in_1 = dwh_in_1.where(dwh_in_1.bool_result == True)
        print("#### count ###")
        get_datetime_now()
        dwh_in_1.show()
        print(dwh_in_1.count())


def insert_data(spark, spark_df, pid, psid, key_obj):
        spark_df.registerTempTable("temp")
        spark.sql("insert into {0}.{1}  select * from temp".format(key_obj['target_database'],key_obj['target_table'].lower()))
        set_write_count(psid, spark.sql("select * from {0}.{1} where process_id = {2}".format(key_obj['target_database'], key_obj['target_table'].lower(), pid)).count())
			
			
			
                                                                                                                                                                                                                   48,45-52      Top
