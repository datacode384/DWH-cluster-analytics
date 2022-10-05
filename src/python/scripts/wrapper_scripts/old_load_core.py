import sys
sys.path.append("..")
from wrapper_scripts.hdfs_functions import file_permission, delete_file, rename_file, delete_dir, dir_permission
from wrapper_scripts.hdfs_functions import ingest as hdfs_ingest
from monitoring.job_monitoring_functions import *
import datetime
from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import findspark
import os
from pyspark.sql.functions import current_date, col
from pyspark.sql import Row 
from wrapper_scripts.hash_gen import md5_hash
from wrapper_scripts.add_columns_to_csv import lower_col_names, add_column
from pyspark.sql.types import *
from typing import List
import re
import json

# GLOBAL VALUES
def ingest(spark, key_obj, pid, psid):

# GLOBAL VALUES
	#pid = startJobExecution(key_obj['tenant_name'], key_obj['application_name'])
#1. INGEST

	#startJobStepExecution(pid, key_obj['table_name'].lower()+'_ingest_'+key_obj['tenant_name']+'_'+key_obj['application_name'])

	hdfs_ingest(key_obj['local_loc'], key_obj['hdfs_loc'])
	file_permission(key_obj['hdfs_file_loc'])
	#setStatusJobStepExecution(getProcessStepID(), key_obj['table_name'].lower()+'_ingest_'+key_obj['tenant_name']+'_'+key_obj['application_name'], 'finished')

####################

def load_rawvault(spark, key_obj, pid, psid ):

#2. LOAD RAWVAULT - adds columns process_id and insert_tst to original csv

	#startJobStepExecution(pid, key_obj['table_name'].lower()+'_load_rawvault_'+key_obj['tenant_name']+'_'+key_obj['application_name'])
	prefixes = ['pas_', 'bpms_', 'par_', 'dwh_']
	prefix = key_obj['table_name'].split("_")[0]
	if key_obj['table_name'].split("_")[0]+"_" in prefixes:
		base_table_name = key_obj['table_name'].partition(key_obj['table_name'].split("_")[0]+"_")[2]
	else:
		base_table_name = key_obj['table_name'].partition("_")[2]
	rawvault = spark.sql("select * from {0}.{1}".format(key_obj['source_database'], key_obj['table_name'].lower()))
	#rawvault.show()
	# delete extra columns added to hive raw_vault table for comparing schema with csv file
	cols_to_drop_for_schema_chk = ["insert_tst", "source_system", "process_id", "tenant_name"]
	rawvault = rawvault.drop(*cols_to_drop_for_schema_chk)
	
	json_rawvault_schema = rawvault.schema.json()

	structType_rawvault_schema = StructType.fromJson(json.loads(json_rawvault_schema))
	bad_record_column = [StructField("bad_record", StringType(), True)]
	customschema = StructType(structType_rawvault_schema.fields + bad_record_column)

	def_tables = ['bom','enum','enumattr','pd','pdtf','pw','risktype','tf','version']

	if prefix == 'pas':
		user = "lfdwh"
		password = "FEIRNffe0E95vRMRT"
		jdbcurl = "jdbc:db2://10.85.200.160:50000/dispodb"
		#user = "db2inst1"
		#password = "password"
		#jdbcurl = "jdbc:db2://10.85.200.151:50000/lfdb"
		driver = "com.ibm.db2.jcc.DB2Driver"
		if base_table_name in def_tables:
			final_table_name = 'def.' + base_table_name
		else:
			final_table_name = 'ebf.' + base_table_name
	else:
		user = "db2inst1"
		password = "3Hk3fVFQkd3LFCzc"
		jdbcurl = "jdbc:db2://10.85.200.180:50000/db01"
		driver = "com.ibm.db2.jcc.DB2Driver"
		final_table_name = "sap_par." + prefix + "_" + base_table_name

#	local_df = (spark.read.format("com.databricks.spark.csv")
#		.option("badRecordsPath", "/shared/HIVE-Table-Data/bad_records/")
#		.option("columnNameOfCorruptRecord", "bad_record")
#		.options(header="true")
#		.csv(key_obj['hdfs_file_loc'], header=True, schema=customschema)
#	)
	local_df = spark.read.format("jdbc")\
	.option('url', jdbcurl)\
	.option('driver', driver)\
	.option('user', user)\
	.option('dbtable', final_table_name)\
	.option('password', password)\
	.option('badRecordsPath', '/shared/HIVE-Table-Data/bad_records/')\
	.option("columnNameOfCorruptRecord", "bad_record")\
	.option("schema", customschema)\
	.load()
	#local_df.printSchema()

	# lower column names in local_df:
	local_df = lower_col_names(local_df)
	
	local_df.show()

	# Works with the assumption we know atleast one non-nullable column in our dataset.
	# This non nullable column can be passed later from our wrapper script
	if 'lvid' not in rawvault.columns:
		filter_column = key_obj['primaryKeys']['id']['fields'][0]
		local = local_df.filter(filter_column + " is NULL")
	else:
		local = local_df.filter("lvid is NULL")

	local.show()

	# write the complete bad_rows spark dataframe to hdfs location below 
	#loc_to_store = '/shared/bad_records' + hdfs_file_loc.split('/')[-1]
	loc_to_store = 'hdfs://10.85.52.12:8020/tmp/ingests/bad_records/'+ key_obj['hdfs_file_loc'].split('/')[-1]

	#print(loc_to_store)

	local\
		.coalesce(1)\
		.write\
		.format('com.databricks.spark.csv')\
		.mode("overwrite")\
		.options(header='true')\
		.save(loc_to_store)

	#change the permission to above dir loacation for any user to rwx
	dir_permission(loc_to_store)
	df = spark.read.format("jdbc")\
	.option('url', jdbcurl) \
	.option('driver', driver)\
	.option('user', user)\
	.option('dbtable', final_table_name)\
	.option('password', password)\
	.option('schema',structType_rawvault_schema)\
	.load()
	df = lower_col_names(df)
	#df = (spark.read.format("csv")
	#	.csv(key_obj['hdfs_file_loc'], header=True, schema=structType_rawvault_schema)
	#	)
	#read count function call for number of records uploaded to hdfs
	read_count = df.count()	
	set_read_count(psid, read_count )
	
	if 'lvid' in df.columns:
		local_df = df.na.drop(how="all")
	else:
		local_df = df
	rawvault_df = (local_df.withColumn('process_id', lit(pid))
			.withColumn('insert_tst', lit(F.current_timestamp()))
			.withColumn('source_system', lit(key_obj['source_system'])))
			#.withColumn('tpa_mandant', lit(key_obj['tenant_name'])))
	#rawvault_df.show()
	cols = []
	rawvault = spark.sql("select * from {0}.{1}".format(key_obj['source_database'],key_obj['table_name']))
	#partitions = ["process_id", "tenant_name"]
	for col in rawvault.columns:
		if col not in key_obj['partitions']:
			cols.append(col)
	cols.extend(key_obj['partitions'])
	rawvault_df = rawvault_df.select(cols)
	rawvault_df.show()
	rawvault_df.registerTempTable("temptable")
	source = spark.sql("select * from {0}.{1}".format(key_obj['source_database'], key_obj['table_name']))
	missingRawvault = []
	for col in rawvault_df.columns:
		if col not in source.columns:
			missingRawvault.append(col)
	print("Columns in core but not in rawvault dataframe: ",missingRawvault)
	missingCore = []
	for col in source.columns:
		if col not in rawvault_df.columns:
			missingCore.append(col)
	print("Columns in rawvault dataframe but not in core table: ",missingCore)
	print("insert into ", key_obj['source_database'], key_obj['table_name'].lower())
	spark.sql("insert into table {0}.{1} select * from temptable".format(key_obj['source_database'], key_obj['table_name'].lower()))

	#write count function call for records written to rawvault
	write_count = spark.sql("select * from {0}.{1} where process_id = {2}".format(key_obj['source_database'], key_obj['table_name'].lower(), pid)).count()
	set_write_count(psid, write_count )
	filter_count = read_count - write_count
	set_filter_count(psid, filter_count )

	if 'lvid' in rawvault_df.columns and 'mandantid' in rawvault_df.columns:
		rawzone_df = spark.sql("select * from {0}.{1}".format(key_obj['source_database'], key_obj['table_name'].lower()))
		rawzone_df.show()
		tpa_mandant = spark.sql("select * from core.ref_tpa_mandant")
		mapped_tenant = rawzone_df.join(tpa_mandant, 'mandantid', 'left_outer').select(['lvid', 'tpa_mandant', 'process_id', 'insert_tst'])
		mapped_tenant.show()
		map_ref_tpa_mandant = spark.sql("select * from core.pas_ref_map_tpa_mandant")
		anti_joined = mapped_tenant.join(map_ref_tpa_mandant, ['lvid','tpa_mandant'], 'left_anti').select(['lvid', 'tpa_mandant', 'process_id', 'insert_tst'])
		anti_joined.show()
		anti_joined.registerTempTable("temporary")
		spark.sql("insert into table core.pas_ref_map_tpa_mandant select * from temporary")
		

# change overwrite to into !! as some dummy data exists already, used overwrite. but overwrite is not allowed in real time
	
	#setStatusJobStepExecution(getProcessStepID(), key_obj['table_name'].lower()+'_load_rawvault_'+key_obj['tenant_name']+'_'+key_obj['application_name'], 'finished')


#################

def load_core(spark, key_obj, pid, psid ):

#3. LOAD CORE - Adds more columns and does hasing

	#startJobStepExecution(pid, key_obj['table_name'].lower()+'_load_core_'+key_obj['tenant_name']+'_'+key_obj['application_name'])

# tenant_name taken from wrapper script
#tenant_name = "ergo"

# application_name taken from wrapper script
#application_name = "dwh"

# source_system is constant for all scripts
#source_system = 201

#sql_query = 'use ' + source_database
#sql_context.sql(sql_query)
	"""
	source = rawvault_df.withColumn("application_name", lit(key_obj['application_name']))\
		.withColumn("source_system", lit(key_obj['source_system']))\
		.withColumn("tenant_name", lit(key_obj['tenant_name']))
	"""
#core_df.registerTempTable("temptable1")

#sql_context.sql("insert overwrite table core.lv partition(process_id) select * from temptable1")

#core_df.write.partitionBy('process_id').saveAsTable("core.vt")

#ID = md5_hash([core_df.MANDANTID, core_df.BEARBID])


#print("core.lvid is hoswing")
#core_df.select("lvid").show()
 

# STRICLTY FOLLOW ALPABETICAL ORDER WHEN PASSING KEYS TO HASH. IF ORDER CHANGE, WE GET ANOTHER HASH

# send all the column names from wrapper script to below 4 keys of id_key, sid, diff_hash and record_hash. 
	

	## select columns to hash 
	source = spark.sql("select * from {0}.{1}".format(key_obj['source_database'], key_obj['table_name'].lower()))
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
	source.show()
		
	#read count function call of records read from rawvault
	read_count = source.count()
	set_read_count(psid, read_count )
	id_columns = source.select(*sorted(key_obj['primaryKeys']['id']['fields']))
	#print("id_cols", id_columns) 
	sid_columns = source.select(*sorted(key_obj['primaryKeys']['sid']['fields']))
	columns_to_drop_for_diff_hash = ['process_id', 'insert_tst']  # get all columns except process_id and insert_tst to diff_hash. use md5. no sha2.
	df = source.drop(*columns_to_drop_for_diff_hash)

	id_key = md5_hash(id_columns.columns)	
	sid = md5_hash(sid_columns.columns)
	diff_hash = md5_hash(df.columns)

	## drop the selected columns for records_hash

	#columns_to_drop_for_record_hash = ['process_id', 'insert_tst', 'lv_sid']  

	#df_record_hash = source.drop(*columns_to_drop_for_record_hash)
	
	source = (source.withColumn(key_obj['primaryKeys']['id']['field_name'], id_key)
                       .withColumn(key_obj['primaryKeys']['sid']['field_name'],sid)
                       .withColumn("diff_hash", diff_hash)
		)

	columns_to_drop_for_record_hash = ['process_id', 'insert_tst', key_obj['primaryKeys']['sid']['field_name']]

	df_record_hash = source.drop(*columns_to_drop_for_record_hash)

	record_hash = md5_hash(df_record_hash.columns)

	source = source.withColumn("record_hash", record_hash)
         
	#source.show()       
	core_df = spark.sql("select * from {0}.{1}".format(key_obj['target_database'],key_obj['table_name']))
	core_df.show()
	
	missingRawvault = []
	for col in core_df.columns:
		if col not in source.columns:
			missingRawvault.append(col)
	print("Columns in core but not in rawvault dataframe: ",missingRawvault)
	missingCore = []
	for col in source.columns:
		if col not in core_df.columns:
			missingCore.append(col)
	print("Columns in rawvault dataframe but not in core table: ",missingCore)

	for fk in key_obj['foreignKeys']:
		fk_hash = md5_hash(key_obj['foreignKeys'][fk])
		source = source.withColumn(fk, fk_hash)

	cols = []
	#partitions = ["process_id", "tenant_name"]
	for col in core_df.columns:
		if col not in key_obj['partitions']:
			cols.append(col)
	cols.extend(key_obj['partitions'])

	source = source.select(cols)
	#!!! REMOVED TENANT_NAME FROM JOIN CONDITION, KEEP IN MIND
	join_condition = [source["diff_hash"] == core_df["diff_hash"], source[key_obj["primaryKeys"]["id"]["field_name"]] == core_df[key_obj["primaryKeys"]["id"]["field_name"]]]
	
	#condition for joining the lv and bearbnw tables
	#join_condition = [source["lvid"] == core_df["lvid"], source["bearbid"] == core_df["bearbid"]]

	print("before join:",datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
	join_result = source.join(core_df, join_condition,'left_anti')
	print("after join:",datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
	
	filter_count = source.count() - join_result.count()
	set_filter_count(psid, filter_count )

	join_result.show()
	join_result.registerTempTable("temp")
	
	#spark.sql("""insert into core.lv_test7 select * from temptable where diff_hash not in (select first_value(diff_hash) over ( partition by id_key order by process_id desc) as diff_hash from core.lv_test7)""")
	print("rows in "+ key_obj['source_database']+" of table "+key_obj['table_name'], spark.sql("select * from {0}.{1}".format(key_obj['source_database'],key_obj['table_name'].lower())).count())
	print("rows in "+ key_obj['target_database']+" of table "+key_obj['table_name'], spark.sql("select * from {0}.{1}".format(key_obj['target_database'],key_obj['table_name'].lower())).count())
	print("rows in temptable to be inserted from: ", spark.sql("select * from temp").count())

	print("before "+key_obj['table_name']+" insert:",datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
	
	print("insert into ", key_obj['target_database'], key_obj['table_name'].lower()) 

	spark.sql("insert into {0}.{1}  select * from temp".format(key_obj['target_database'],key_obj['table_name'].lower())) 
	## Insert new data from rawvault to core after adding above hash keys and following insert rules as specified in confluence docs 
	#spark.sql("insert into {0}.{1} select * from temptable where diff_hash not in (select distinct first_value(diff_hash) over ( partition by {2},{3} order by {4} desc) as diff_hash from {0}.{1})".format(key_obj['target_database'], key_obj['table_name'].lower(), key_obj['primaryKeys']['id']['field_name'] ,"bearbid", "process_id"))

	print("after "+key_obj['table_name']+" insert:",datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

	write_count = spark.sql("select * from {0}.{1} where process_id = {2}".format(key_obj['target_database'], key_obj['table_name'].lower(), pid)).count()                                                                                                                
	set_write_count(psid, write_count )
# writing spark df to hive directly
#source1.write.mode("append").insertInto("core.lv_test7")

# sql way of insertion to hive
#spark.sql("""insert into table core.lv_test7 partition(process_id="1",tenant_name="ergo") select lvid,pdid,generation,mandantid,verzinsbeginn,zweiink,zinsbdep,bearbid,bearbidabg,guebiszins,betrag,waehrungid,lvbegt,lvablt,trdk,btrdiffmig,btrabgl,vstkk,uebverr,lvjahrtag,lvstatus,zb,vzborig,kzvorg,kz_anr,anrewert,annahmeart,fr_switch,let_switch,kz_versst,versst,rentbekz,vertriebsweg,versstnate,errbtgniv,minbtg,zweivorza,btrdiffwsw,kznachz,uebverrnsp,kzanbieterwechsel,kzrelease,antragssteuerung,partkey,kzangvers,kzauszstop,kzabfindungkbr,kollnr,lfdkollnr,verwgrpnr,musterid,zuzahlpol,isvoraussschaedverw,kistamwirkdat,kistambearbdat,steulandsekundaer,steuzuschlagzuzahl,steuzuschlag,steulandprimaer,kzrechenkern,vorgperformance,c_werbhilf,c_zugweg,insert_tst,application_name,source_system,id_key,sid,diff_hash,record_hash from temp where process_id="1" and tenant_name="ergo" """)



	#setStatusJobStepExecution(getProcessStepID(), key_obj['table_name'].lower()+'_load_core_'+key_obj['tenant_name']+'_'+key_obj['application_name'], 'finished')


#setStatus()
	#setStatus(pid, 'finished')

