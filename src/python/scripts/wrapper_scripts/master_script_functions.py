from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from wrapper_scripts.hash_gen import md5_hash

def joins(spark, source, key_obj):
	for joins in key_obj['join_tables']:
		columns_needed_target = []
		target_table = joins['joined_table']
		target_db = joins['join_schema']
		target_df = spark.sql('select * from {0}.{1}'.format(target_db, target_table))
		columns_needed_target.extend(joins['join_columns'])
		for old,new in joins['new_columns']:
			columns_needed_target.append(old)
		target_df = target_df.select(*columns_needed_target)
		for old,new in joins['new_columns']:
			target_df = target_df.withColumnRenamed(old,new)
		source = source.join(target_df, joins['join_columns'], joins['join_type'])
	return source

def hashes(spark, source, key_obj):
	#assign columns to be hashed to hash variables
	id_columns = source.select(*sorted(key_obj['primaryKeys']['id']['fields']))
	sid_columns = source.select(*sorted(key_obj['primaryKeys']['sid']['fields']))
	columns_to_drop_for_diff_hash = ['process_id', 'insert_tst']  # get all columns except process_id and insert_tst to diff_hash. use md5.
	df = source.drop(*columns_to_drop_for_diff_hash)

	id_key = md5_hash(id_columns.columns)
	sid = md5_hash(sid_columns.columns)
	diff_hash = md5_hash(df.columns)

	## drop the selected columns for records_hash

	source = (source.withColumn(key_obj['primaryKeys']['id']['field_name'], id_key)
		       .withColumn(key_obj['primaryKeys']['sid']['field_name'],sid)
		       .withColumn("diff_hash", diff_hash)
		)

	columns_to_drop_for_record_hash = ['process_id', 'insert_tst', key_obj['primaryKeys']['sid']['field_name']]

	df_record_hash = source.drop(*columns_to_drop_for_record_hash)

	record_hash = md5_hash(df_record_hash.columns)

	source = source.withColumn("record_hash", record_hash)
	
	return source

def foreign_keys(spark, source, key_obj):
	for fk in key_obj['foreignKeys']:
		fk_hash = md5_hash(key_obj['foreignKeys'][fk])
		source = source.withColumn(fk, fk_hash)
	return source
