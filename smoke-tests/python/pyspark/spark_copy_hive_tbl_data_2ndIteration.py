# create a hive table using spark.sql
# insert data into this table
# copy this table or another table

#spark.sql("CREATE TABLE new_db.new_table AS SELECT *FROM old_db.old_table;")

from pyspark.sql import SparkSession
import json

data = json.load(open("config.json","r"))

for x in data["spark"]:
	spark = SparkSession.builder.config(x,data["spark"][x]).enableHiveSupport().getOrCreate()

spark.sql("DROP TABLE IF EXISTS default.sample") 
 
spark.sql("DROP TABLE IF EXISTS default.sample1") 

spark.sql("CREATE TABLE IF NOT EXISTS  default.sample (col1 string, col2 string, col3 string) STORED AS parquet")

spark.sql("INSERT INTO default.sample select t.* from (select 'w1',  'w2', 'w3') t")

spark.sql("CREATE TABLE default.sample1 AS select * from default.sample")
