"""
Connect to hive using jdbc and import databases directly to spark dataframes 

usage: spark-submit --driver-class-path /opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/jars/HiveJDBC41.jar --jars /opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/jars/HiveJDBC41.jar --class com.cloudera.impala.jdbc41.Driver hive_jdbc_connection.py

Author: krishna damarla
"""

from spark_cfg import spark_init
import numpy
import datetime
import jaydebeapi as db2
import os

properties = {
"drivers": "org.apache.hive.jdbc.HiveDriver"
}

spark = spark_init()

db_df = spark.read.jdbc(url= 'jdbc:hive2://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:10000/raw_vault', table ='jurlv', properties = properties)

db_df.show()
