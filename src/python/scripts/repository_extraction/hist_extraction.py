"""
This program contains the functions used for db extraction
Useful link for db2 operations: https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.swg.im.dbclient.python.doc/doc/t0054388.html
Usage: python db_extraction.py
"""
import ibm_db as db2
import sys
sys.path.append("..")
from wrapper_scripts.connect import connect_db02
from wrapper_scripts.hdfs_functions import run_cmd
import os
import re
#

def target_data_model(target_directory, con):
	print("Executing target_data_model function")
	res = db2.exec_immediate(con,"SELECT TABLE_ID,DDL from TABLE_REPO.V_DDL_CORE_HIST")                                                                                                                                              
	while db2.fetch_row(res)!=False:
		table_id = db2.result(res,"TABLE_ID")
		if table_id is not None:
			with open(target_directory+"/"+table_id.lower()+".hql","w+") as f:
				f.write(str(db2.result(res,"DDL")))

def export_all():
	#run_cmd(['python','job_properties.py','test_file.txt','JS_ETL_NEU_Mapping.UTF8.properties'])
	#iws_job_chain(os.getcwd()+"/target/TEST_EXEC",connect_db02(),"PROD")	
	#iws_job_chain(os.getcwd()+"/target/ITERGO",connect_db02(),"PROD")
	#wrapper_script(os.getcwd(),connect_db02())
	#wrapper_script(os.getcwd(),connect_db02())
	#load_all(os.getcwd(), connect_db02())
	target_data_model(os.getcwd(),connect_db02())

export_all()
