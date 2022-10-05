from behave import *
from pyspark.sql import SparkSession

@given('a spark session')
def step_impl(context):
	context.spark = (
		SparkSession.builder
			.config("spark.debug.maxToStringFields", "10000")
			.config("spark.hadoop.hive.exec.dynamic.partition.mode", "non-strict")
			.config("spark.hadoop.hive.exec.dynamic.partition", "true")
			.config("spark.sql.warehouse.dir", "hdfs://10.85.52.12:8020/user/hive/warehouse")
			.config("hive.metastore.warehouse.dir", "hdfs://10.85.52.12:8020/user/hive/warehouse")
			.config("hive.exec.dynamic.partition.mode", "nonstrict")
			.config("hive.exec.dynamic.partition", "true")
			.config("spark.sql.catalogImplementation", "hive")
			.config("hive.metastore.uris", "thrift://10.85.52.12:9083")
			.config("spark.sql.hive.metastore.sharedPrefixes", "org.postgresql")
			.config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:postgresql://10.85.93.209:5432/hive")
			.config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "jdbc:postgresql://10.85.93.209:5432/hive")
			.config("spark.hadoop.javax.jdo.option.ConnectionUserName", "hive")
			.config("spark.hadoop.javax.jdo.option.ConnectionPassword", "hive")
			.enableHiveSupport()
			.getOrCreate()
)
	return context.spark