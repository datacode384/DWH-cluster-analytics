"""
Connect to impala using jdbc and import databases directly to spark dataframes 

usage: spark-submit --driver-class-path /opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/jars/ImpalaJDBC41.jar --jars /opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/jars/ImpalaJDBC41.jar --class com.cloudera.impala.jdbc41.Driver impala_jdbc_connection.py

Author: krishna damarla
"""

from spark_cfg import spark_init
from job_monitoring_functions import *
from create_table_permissions import create_table_permissions
from load_core import ingest
import traceback
import numpy
import datetime
import jaydebeapi as db2
import os


properties = {
"drivers": "com.cloudera.impala.jdbc41.Driver"
}

spark = spark_init()

db_df = spark.read.jdbc(url= 'jdbc:impala://dwh-hdp-node02.dev.ergo.liferunoffinsuranceplatform.com:21050/raw_vault', table ='jurlv', properties = properties)

db_df.show()
