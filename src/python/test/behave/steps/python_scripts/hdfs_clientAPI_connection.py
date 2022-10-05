"""
connect to remote / local hdfs with pywebhdfs client api

usage: python hdfs_clientAPI_connection.py

Author: krishna damarla

"""
from pywebhdfs.webhdfs import PyWebHdfsClient #https://pythonhosted.org/pywebhdfs/
from pprint import pprint

hdfs = PyWebHdfsClient(host='10.85.52.14',port='9870')  # your Namenode IP

print(hdfs)

myfile = '/tmp/ingest/BEARBNW.csv'

mydir = '/tmp/ingest/test'

#dir_create = hdfs.make_dir(mydir)

file_status = hdfs.get_file_dir_status(myfile)
print(file_status)

#print(dir_create)
