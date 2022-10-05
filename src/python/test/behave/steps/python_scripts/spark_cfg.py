import findspark
import os
findspark.init(spark_home="/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")


#os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_181-cloudera/"

#os.environ["PATH"] = "/usr/java/jdk1.8.0_181-cloudera/bin"

#os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark"

#os.environ["PYSPARK_PYTHON"] = "/usr/local/37Pro/bin/python"

os.environ["PYTHONPATH"] ="/usr/local/37Pro/lib/python3.7/site-packages"

#findspark.init(spark_home="spark")
#import pyspark

from pyspark.sql import SparkSession

def spark_init():
	spark = (
        SparkSession.builder
        .config("spark.debug.maxToStringFields", "10000")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "non-strict")
	.config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
	.config("hive.exec.dynamic.partition", "true")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.11:0.5.0")
        .config("spark.sql.warehouse.dir","hdfs://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:8020/user/hive/warehouse")
        .config("hive.metastore.warehouse.dir", "hdfs://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:8020/user/hive/warehouse")
        .config("hive.metastore.uris","thrift://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:9083")
        .enableHiveSupport()
        .getOrCreate() 
        )
	return spark
