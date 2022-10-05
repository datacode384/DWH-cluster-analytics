from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import sys
import json
import os
import findspark
import datetime

def read_local_df(spark, local_path):
	local_df = (spark.read.format("com.databricks.spark.csv")
		#.option("badRecordsPath", "/shared/HIVE-Table-Data/bad_records/")
		#.option("columnNameOfCorruptRecord", "bad_record")
		.options(header="true")
		.options(inferschema=True)
		.csv(local_path, header=True)
	)
	return local_df


def add_column(df, column_name, value):
	return df.withColumn(column_name, value)

def write_df_to_file(output_name, df):
	df.write.mode("overwrite").\
		format("com.databricks.spark.csv").\
		options(header="true").\
		save(output_name)

def write_df_to_hive(db_name, table_name, df):
	db_and_table = "{0}.{1}".format(db_name, table_name)
	df.write.mode("overwrite")\
		.format("com.databricks.spark.csv")\
		.partitionBy("process_id")\
		.option("header","true")\
		.saveAsTable(db_and_table)

def write_df_to_hdfs(hdfs_path, df):
	df.write.mode("overwrite")\
		.format("com.databricks.spark.csv")\
		.partitionBy("process_id")\
		.saveAsTable(hdfs_path)

def possible_writes_hive(db_name, table_name, df):
	db_and_table = "{0}.{1}".format(db_name, table_name)
	#spark_df.write.mode("overwrite").saveAsTable(db_and_corresponding_table)

def lower_col_names(df):
	for col in df.columns:
		    df = df.withColumnRenamed(col, col.lower())
	return df

def add_columns(table_name, pid):
	local_csv = read_local_df(spark, '/shared/original/' + table_name + '.csv')
	local_output_path = '/shared/rawvault/' + table_name
	local_csv = add_column( add_column(local_csv, 'insert_tst', F.current_timestamp()), 'process_id', pid)

	local_csv = lower_col_names(local_csv)

	write_df_to_file(local_output_path, local_csv)
