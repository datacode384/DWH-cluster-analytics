"""
This program formats the db2 ddl tablename.sql files in ../db2_ddl_sql folder to a format relevant to hive ddl by replacing some of the notations.
The hive hql notation is printed in command prompt for furhther modifications. 
usage: python convert_db2_to_hive_ddl.py ../db2_ddl_sql/tablename.sql
Author: Alexandru Coban
"""

import subprocess
import sys
import string

if len(sys.argv) != 2:
        print ( "usage: python script.py filename_of_query")
        exit (0)
else:
        filename = sys.argv[1]

query = ''

#try:
with open(filename, 'r') as f:
        query = f.read()
#except:
#       print('file not found, try again')
#       exit(0)

"""
#run linux commands
def run_cmd(args_list):
        if type(args_list) == list:
                print('Running system command: {0}'.format(' '.join(args_list)))
                proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                s_output, s_err = proc.communicate()
                s_return =  proc.returncode
        elif type(args_list) == str:
                return args_list
        else:
                print ('wrong type in function run_cmd')
                exit(0)
"""
query = (query.replace('  ', '')).replace('     ', '').replace('   ', '').replace('\n', '').replace('"', '').replace('NOT NULL', '')

print(query)

#command = 'hive -e ' + '\'' + query + '\''
#print ('running command: \n', command)

#l = run_cmd(query)
