"""
python file_permissions.py source_database target_database table_name file_name
"""
from hdfs_functions import run_cmd, dir_permission
import sys
import traceback

def file_permissions(source_database, target_database, table_name):
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
		file_permissions(source_database, target_database, table_name)
	except:
		print(traceback.format_exc())
