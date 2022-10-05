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
def iws_job_chain(target_directory, con, job_chain):
	print("Executing iws_job_chain function")
	res = db2.exec_immediate(con,"SELECT * from JOB_REPO.V_IWS_JOB_CHAIN_TENANT")
	while db2.fetch_row(res)!=False:
		result = db2.result(res,"XML_JOB")
		tenant_name = db2.result(res,"TENANT_NAME")
		os.makedirs(os.getcwd()+"/target/"+tenant_name, exist_ok=True)
		if tenant_name in target_directory:
			with open(target_directory+"/"+ tenant_name + "_res.xml","w+") as f:
				f.write(result)
		else:
			continue

#iws_job_chain(os.getcwd()+"/target/TEST_EXEC",connect_db02(),"PROD")
#iws_job_chain(os.getcwd()+"/target/ITERGO",connect_db02(),"PROD")

def wrapper_script(target_directory, con):
	print("Executing wrapper_script function")
	res = db2.exec_immediate(con,"SELECT JOB_NAME,SOURCE_CODE from JOB_REPO.V_GENERATED_SCRIPTS_FINAL where ENVIRONMENT_NAME='DEV'")
	#os.makedirs(os.getcwd()+"/target/", exist_ok=True)
	source_system = ['pas_', 'bpms_', 'par_', 'dwh_']
	while db2.fetch_row(res)!=False:
		job_name= db2.result(res,"JOB_NAME")
		wrapper_type=""
		if "_LF_" in job_name:
			if "ZONE" in job_name:
				table_name = job_name.partition("ZONE_")[2]
				wrapper_type ="rawvault"
			elif "CORE" in job_name:
				table_name = job_name.partition("CORE_")[2]
				wrapper_type="core"
			if "ITERGO_JC" in job_name:
				print(target_directory+"/"+"itergo_jc"+job_name[job_name.rfind("_"):].lower()+".py")
				with open(target_directory+"/"+"itergo_jc"+job_name[job_name.rfind("_"):].lower()+".py","w+") as f:
					f.write(db2.result(res,"SOURCE_CODE"))
				continue
			else:
				#for i in source_system: 
					#if i in job_name:			   
				#with open(target_directory+"/"+job_name[job_name.rfind("_")+1:].lower()+"_wrapper_"+ wrapper_type+".py","w+") as f:
				#print("test: ", job_name.partition(i)[2])
				with open(target_directory+"/"+table_name.lower()+"_wrapper_"+ wrapper_type+".py","w+") as f:
					f.write(db2.result(res,"SOURCE_CODE"))
				with open(target_directory+"/"+table_name.lower()+"_wrapper_"+ wrapper_type+".py","r+") as f:
					content= f.read()
					f.seek(0)
					f.write(re.sub(r'\@\@\@.*?\@\@\@', '', content))
					f.truncate()

#wrapper_script(os.getcwd()+"/target",connect_db02())

def target_data_model(target_directory, db, con):
	print("Executing target_data_model function")
	database= db
	#os.makedirs(os.getcwd()+"/target/"+database, exist_ok=True)
	if db is "raw_zone":
		res = db2.exec_immediate(con,"SELECT TABLE_NAME_DWH, DDL from TABLE_REPO.V_DDL_RAW_VAULT WHERE TABLE_NAME_DWH like 'PAS_%'")
	else:
		res = db2.exec_immediate(con,"SELECT TABLE_NAME_DWH,DDL from TABLE_REPO.V_DDL_"+database+" WHERE TABLE_NAME_DWH like 'PAS_%'")
	while db2.fetch_row(res)!=False:
		table_id = db2.result(res,"TABLE_NAME_DWH")
		if table_id is not None:
			with open(target_directory+"/"+table_id.lower() +"_"+database.lower()+".hql","w+") as f:
				f.write(str(db2.result(res,"DDL")))
	if db is "raw_zone":
		res = db2.exec_immediate(con,"SELECT TABLE_NAME_DWH, DDL from TABLE_REPO.V_DDL_RAW_VAULT WHERE TABLE_NAME_DWH like 'PAS_%'")
	else:
		res = db2.exec_immediate(con,"SELECT TABLE_NAME_DWH,DDL from TABLE_REPO.V_DDL_"+database+" WHERE TABLE_NAME_DWH like 'PAS_%'")                                                                                                                                              
	while db2.fetch_row(res)!=False:
		table_id = db2.result(res,"TABLE_NAME_DWH")
		if table_id is not None:
			with open(target_directory+"/"+table_id.lower() +"_"+database.lower()+".hql","w+") as f:
				f.write(str(db2.result(res,"DDL")))

#target_data_model(os.getcwd()+"/target/core",connect_db02())
#target_data_model(os.getcwd()+"/target/raw_vault",connect_db02())
def load_all(target_directory, con):
	print("Executing load_all function")
	source_system = ['pas_', 'bpms_', 'par_', 'dwh_']
	with open(target_directory+"/"+"load_all.py","w+") as f:
		iterations = [1,2]
		tables =[]
		jc_start ="itergo_jc_start.py"
		jc_stop = "itergo_jc_stop.py"
		exceptions = ["pas_bearbnw"]
		f.write("from hdfs_functions import *\n")
		for iteration in iterations:
			if iteration is 2:
				tab='tables =["' + '","'.join(sorted(tables))+'"]\n\n'
				f.write(tab)
				f.write("l, p, error = run_cmd(['python','"+jc_start+"'])\n")
				f.write("print(l,p,error)\n")
			for i in exceptions:
				ss=i.split("_")[0]
				tn=i.partition(ss+"_")[2]
				if tn not in tables:
					tables.append(tn)
				cond="if '"+tn+"' in tables:\n"
				rz_call="\tl,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/"+i+"_wrapper_rawvault.py'])\n\tprint(l,p,error)\n"
				c_call="\tl,p,error = run_cmd(['spark-submit','core/"+i+"_wrapper_core.py'])\n\tprint(l,p,error)\n"
				if iteration is 2:
					f.write(cond)
					f.write(rz_call)
					f.write(c_call)
			res = db2.exec_immediate(con,"SELECT VALUE,STRING_FLAG from JOB_REPO.PARAMETER_GROUP_VALUE where PARAMETER_GROUP_ID = 'LF_TABLE'")
			while db2.fetch_row(res)!=False:
				table_name= db2.result(res,"VALUE")
				flag = db2.result(res,"STRING_FLAG")
				ss=table_name.split("_")[0]
				tn = table_name.partition(ss+"_")[2]
				if tn not in tables:
					tables.append(tn)
				if flag is 1:
					condition= "if '"+tn+"' in tables:\n"
					rawzone_call="\tl,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/"+table_name+"_wrapper_rawvault.py'])\n\tprint(l,p,error)\n"
					core_call=   "\tl,p,error = run_cmd(['spark-submit','core/"+table_name+"_wrapper_core.py'])\n\tprint(l,p,error)\n"
					if iteration is 2:
						f.write(condition)
						f.write(rawzone_call)
						f.write(core_call)
		f.write("l,p,error = run_cmd(['python','"+jc_stop+"'])\n")
#load_all(os.getcwd()+"/target",connect_db02())

def export_all():
	#run_cmd(['python','job_properties.py','test_file.txt','JS_ETL_NEU_Mapping.UTF8.properties'])
	#iws_job_chain(os.getcwd()+"/target/TEST_EXEC",connect_db02(),"PROD")	
	#iws_job_chain(os.getcwd()+"/target/ITERGO",connect_db02(),"PROD")
	#wrapper_script(os.getcwd(),connect_db02())
	#wrapper_script(os.getcwd(),connect_db02())
	load_all(os.getcwd(), connect_db02())
	#target_data_model(os.getcwd(), "core",connect_db02())
	#target_data_model(os.getcwd(),"raw_zone",connect_db02())

export_all()
