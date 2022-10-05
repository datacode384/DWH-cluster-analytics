import datetime
import calendar
import json
from pyspark.sql.types import *
from pyspark.sql import SparkSession, SQLContext, HiveContext
from hdfs_functions import ingest, file_permission
from spark_cfg import spark_init

def quarter(date):
	if date >= 1 and date <= 3:
		return 'Q1'
	if date >= 4 and date <= 6:
		return 'Q2'
	if date >= 7 and date <= 9:
		return 'Q3'
	if date >= 10 and date <= 12:
		return 'Q4'

def month_end(date):
	last_day = calendar.monthrange(date.year, date.month)[1]
	if date.day == last_day:
		return True
	return False

def qtr_end(date):
	if date.month in [3,6,9,12] and month_end(date):
		return True
	return False

def year_end(date):
	if date.month == 12 and month_end(date):
		return True
	return False

spark = spark_init()
"""
f = open('ref_zeit.csv','w')
f.write('"zeit_id","zeit_tst","jahr_qtr","jahr_mon","flag_monat_ende","flag_quartal_ende","flag_jahr_ende"\n')

i_date = datetime.date(1900,1,1)
day = datetime.timedelta(days=1)
zeit_id = 1

#while (i_date < datetime.date(2100,1,1)):
while (i_date < datetime.date(2100,1,1)):
	t_date = datetime.datetime(i_date.year, i_date.month, i_date.day, 00, 00, 00, 0000)
	zeit_tst = t_date.strftime("%Y-%m-%d %H:%M:%S.%f")
	mon = i_date.month
	qtr = quarter(mon)
	jahr = i_date.year
	jahr_qtr = '{0}_{1}'.format(jahr, qtr)
	if mon <10:
		jahr_mon = '{0}_0{1}'.format(jahr, mon)
	else: 
		jahr_mon = '{0}_{1}'.format(jahr, mon)
	monat_ende = month_end(i_date)
	qtr_ende = qtr_end(i_date)
	jahr_ende = year_end(i_date)
	#print('insert into {0}.{1} values({2}, {3}, {4}, {5}, {6}, {7}, {8});'.format(db, table, zeit_id, zeit_tst, jahr_qtr, jahr_mon, monat_ende, qtr_ende, jahr_ende))
	line = '{0},"{1}","{2}","{3}",{4},{5},{6}\n'.format(zeit_id, zeit_tst, jahr_qtr, jahr_mon, monat_ende, qtr_ende, jahr_ende)
	f.write(line)
	zeit_id += 1
	i_date += day
f.close()
"""
db = 'core'
table = 'ref_zeit'
local_path = "/shared/original/ref_zeit.csv"
#ingest(local_path, "/tmp/ingests")
#file_permission("/tmp/ingests/ref_zeit.csv")

core_df = spark.sql("select * from {0}.{1}".format(db, table))
json_schema = core_df.schema.json()
core_schema = StructType.fromJson(json.loads(json_schema))

df = spark.read.format("com.databricks.spark.csv").csv("/tmp/ingests/ref_zeit.csv", header=True, schema=core_schema)
df.show()
df.registerTempTable("temp")
spark.sql("insert into table {0}.{1} select * from temp".format(db, table))
