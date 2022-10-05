import unittest
import sys 
sys.path.append('../python')
from jurlv_wrapper import jurlv_wrapper
from job_monitoring_functions import startJobExecution, setStatus
from pyspark.sql import SparkSession

class jurlv_wrapper_UnitTestCase(unittest.TestCase):
	def setUp(self):
		self.pid = startJobExecution("ergo", "dwh")
		self.spark = spark = SparkSession.builder.config('spark.jars.packages', 'com.databricks:spark-xml_2.11:0.5.0').config("spark.sql.warehouse.dir","hdfs://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:8020/user/hive/warehouse").config("hive.metastore.warehouse.dir", "hdfs://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:8020/user/hive/warehouse").config("hive.metastore.uris","thrift://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:9083").config("hive.exec.dynamic.partition.mode", "non-strict").config("hive.exec.dynamic.partition", "true").config("spark.debug.maxToStringFields","10000").enableHiveSupport().getOrCreate()
	
	def test_jurlv_wrapper_call(self):
		self.assertEqual(jurlv_wrapper(self.spark, self.pid),"Wrapper executed successfully")

	def tearDown(self):
		setStatus(self.pid, "unit test")

if __name__ == "__main__":
	unittest.main()
