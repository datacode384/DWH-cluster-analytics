from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import sys
import json
import os
import findspark
import datetime



findspark.init()

spark = SparkSession.builder.config('spark.jars.packages', 'com.databricks:spark-xml_2.11:0.5.0').config("spark.sql.warehouse.dir","hdfs://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:8020/user/hive/warehouse").config("hive.metastore.warehouse.dir", "hdfs://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:8020/user/hive/warehouse").config("hive.metastore.uris","thrift://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:9083").enableHiveSupport().getOrCreate()
spark.conf.set("spark.debug.maxToStringFields", 10000)

spark.conf.set("hive.mapred.mode", "nonstrict")
spark.conf.set("hive.optimize.ppd","true")
spark.conf.set("hive.optimize.index.filter","true")
spark.conf.set("hive.tez.bucket.pruning","true")
spark.conf.set("hive.explain.user","false")
spark.conf.set("hive.fetch.task.conversion","none")
spark.conf.set("hive.support.concurrency","true")
spark.conf.set("hive.txn.manager","org.apache.hadoop.hive.ql.lockmgr.DbTxnManager")
spark.conf.set("hive.exec.orc.split.strategy","BI")

sc = spark.sparkContext
sc.setSystemProperty("hive.metastore.warehouse.dir", "hdfs://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com/user/hive/warehouse")
sql = SQLContext(sc)

user = "dbuser1"
password = "Xbv7J6A8RdqkP8mV"
jdbcurl = "jdbc:db2://10.85.200.175:50000/db01"
table = "lv"



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

if __name__ == "__main__":
	
	local_csv = read_local_df(spark, '/shared/original/' + sys.argv[1])
	local_output_path = '/shared/rawvault/' + (sys.argv[1]).split('/')[-1]
	hdfs_output_path = 'hdfs://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:8020/tmp/ingest/HIVE-Table-Data/' + sys.argv[1].split('/')[-1]
	print ('##############################')
	print (hdfs_output_path)
	local_csv = add_column( add_column(local_csv, 'insert_tst', F.current_timestamp()), 'process_id', lit(os.getpid()) )

	local_csv = lower_col_names(local_csv)	
	
	write_df_to_file(local_output_path, local_csv) # writes parts in a folder named as output_path
	local_csv.show()
	#write_df_to_hive('rawvault', 'lv_test2_alex',local_csv)
	#print(local_csv.schema)

	#db_and_corresponding_table_orc = "{0}.{1}".format("rawvault", "lv_alex_orc_nopart")

	#db_and_corresponding_table_parquet = "{0}.{1}".format("rawvault", "lv_alex_parquet_nopar")

	# save as orc table
	#local_csv.write.mode("overwrite").format("orc").saveAsTable(db_and_corresponding_table_orc)
	# save as parquet
	#local_csv.write.mode("overwrite").format("parquet").saveAsTable(db_and_corresponding_table_parquet)
