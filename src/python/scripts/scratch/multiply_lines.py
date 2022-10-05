#usage:
#spark-submit multiply_lines.py /csv/location/in/hdfs 100000
#where 100000 is your desired number of lines, the number can only go up for now, will update to be able to remove random lines as well at some point

from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql.types import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import numpy as np
import pandas as pd
import random
import sys
import json
import os
import findspark

spark = (SparkSession.builder.config("spark.jars.packages", "com.databricks:spark-xml_2.11:0.5.0").enableHiveSupport().getOrCreate())
sc = spark.sparkContext
spark.conf.set("spark.debug.maxToStringFields", 10000)
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

def read_hdfs_rdd(spark, local_path):
	local_df = (spark.read.format("com.databricks.spark.csv")
		#.option("badRecordsPath", "/shared/HIVE-Table-Data/bad_records/")
		#.option("columnNameOfCorruptRecord", "bad_record")
		.options(header="true")
		.options(inferschema=True)
		.csv(local_path, header=True)
	)
	return local_df

def read_local_pandas_df(path):
	data = pd.read_csv(path, sep=',', dtype=np.str, index_col=False)
	return data

def add_line_to_rdd(df, line):
	aux = spark.createDataFrame(line, spark_df.schema.names)
	return df.union(aux)
def alter_rdd_line(line, col_name, new_value):
	line = line.asDict()
	line[col_name] = new_value
	return Row(**line)

def add_line_to_df(df, line):
	return pd.concat([df, line], sort=False)
def alter_line(line, col_name, new_value):
	line.at[0, col_name] = str(new_value)
	return line

if __name__ == '__main__':

	path = ''
	line_no = 0
	try:
		path = sys.argv[1]
		line_no = sys.argv[2]
	except:
		print('wrong usage, read comment in py file')
		sys.exit()
	try:
		line_no = int(line_no)
	except:
		print('wrong usage, read comment in py file')
		sys.exit()
	
	filename = path.split('/')[-1].lower()

	df = read_local_pandas_df(path)
	#df = df.astype({'LVID': 'int', 'BEARBID': 'int'})
	line_count = len(df.index)

	print ('number of lines in df:', str(line_count))
	print ('desired number of lines:', str(line_no))


	#repeat line adding process while desired line number is not hit
	bearbid_value = 1
	lvid_value = 1
	random_line = df.sample(n=1)
	while(line_count < line_no):
		#random_line = df.sample(n=1)
		index = random_line.index[0]
		random_line.at[index,'BEARBID'] = bearbid_value
		random_line.at[index, 'LVID'] = lvid_value
		df = pd.concat([df, random_line], sort=False)
		line_count += 1
		bearbid_value += 1
		if bearbid_value >= 50:
			bearbid_value = 1
			lvid_value += 1
			print(lvid_value)

		#print(random_line)

	#writing to local file:
	outname = str(line_no) + '_' + filename
	outdir = '~'
	if not os.path.exists(outdir):
		os.mkdir(outdir)

	fullname = os.path.join(outdir, outname)    

	df.to_csv(fullname, index=False)


