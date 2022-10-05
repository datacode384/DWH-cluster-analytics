"""

This program contains various hdfs_functions like stage data from local to hdfs environment, file permissions, delete files, renames files etc. 

Usage: from hdfs_functions import ingest, file_permission, delete_file, rename_file

Author: krishna damarla

"""
import subprocess
import sys

def run_cmd(args_list):
        """
        run linux commands
        """
        print('Running system command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        return s_return, s_output, s_err

#(l,p,k) = run_cmd(['hdfs', 'dfs', '-ls', '/'])

def ingest(local, remote):
	# hadoop fs -put -f /shared/original/LV_test_badrecords_added.csv /tmp/ingest
	l, p, error = run_cmd(['sudo','-u','hdfs', 'hdfs','dfs', '-put', '-f', local, remote])
	print(error)

def file_permission(remote):
	#sudo -u hdfs hdfs dfs -chmod 777 /tmp/ingest/LV_test_badrecords_added.csv
	l, p, error = run_cmd(['sudo','-u','hdfs', 'hdfs','dfs' , '-chmod', '777', remote])
	print(error)
	#sudo -u hdfs hdfs dfs -chown hdfs:supergroup /tmp/ingest/LV_test_badrecords_added.csv
	l, p, error = run_cmd(['sudo','-u','hdfs', 'hdfs','dfs' , '-chown', 'hdfs:supergroup', remote])
	print(error)	

def dir_permission(remote):
	#sudo -u hdfs hdfs dfs -chmod -R 777 /tmp/ingest/LV_test_badrecords_added.csv
	l, p, error = run_cmd(['sudo','-u','hdfs', 'hdfs','dfs' , '-chmod', '-R', '777', remote])
	#sudo -u hdfs hdfs dfs -chown -R hdfs:supergroup /tmp/ingest/LV_test_badrecords_added.csv
	l, p, error = run_cmd(['sudo','-u','hdfs', 'hdfs','dfs' , '-chown', '-R', 'hdfs:supergroup', remote])
	print(error)

def delete_file(remote):
	#sudo -u hdfs hdfs dfs -rm /tmp/ingest/LV_test_badrecords_added.csv
	run_cmd(['sudo','-u','hdfs', 'hdfs','dfs' , '-rm', remote])

def delete_dir(remote):
	#sudo -u hdfs hdfs dfs -rm -R /tmp/ingest/LV_test_badrecords_added.csv
	run_cmd(['sudo','-u','hdfs', 'hdfs','dfs' , '-rm', '-R', remote])

def rename_file(remote):
	#hadoop fs -mv old_file new_file
	run_cmd(['sudo','-u','hdfs', 'hdfs','dfs', '-mv', old_file_name, new_file_name])
