"""
Script that runs hive -f on the .hql files for one table and gives hdfs:supergroup permissions to the freshly created tables in hdfs
Usage: import create_table_permissions(source_database, target_database, table_name, file_name) 
or if run separately:
python create_table_permissions.py source_database target_database table_name file_name
"""
from hdfs_functions import run_cmd, dir_permission
import sys
import traceback

def create_table_permissions(source_database, target_database, table_name):

	try:
		l, p , error = run_cmd(["hive", "-f", "../hql/"+ table_name+"_raw_zone_ddl.hql"])
		print(error)
	except:
		print(traceback.format_exc())

	try:
		l, p, error = run_cmd(["hive", "-f", "../hql/"+ table_name +"_core_ddl.hql"])
		print(error)
	except:
		print(traceback.format_exc())
	
	try:
		run_cmd(["hive", "-f", "../hql/key_"+table_name.upper()+"_create.hql"])
		print(error)
	except:
		print(traceback.format_exc())

	try:
		dir_permission("/user/hive/warehouse/"+source_database+".db/"+table_name)
	except:
		print(traceback.format_exc())

	try:	
		dir_permission("/user/hive/warehouse/"+target_database+".db/"+table_name)
	except:
		print(traceback.format_exc())

	try:
		dir_permission("/user/hive/warehouse/"+target_database+".db/key_"+table_name)
	except:
		print(traceback.format_exc())

if __name__ == "__main__":
	source_database = sys.argv[1]
	target_database = sys.argv[2]
	table_name = sys.argv[3]
	try:
		create_table_permissions(source_database, target_database, table_name)
	except:
		print(traceback.format_exc())
