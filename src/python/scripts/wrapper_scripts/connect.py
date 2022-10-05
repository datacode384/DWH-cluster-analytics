"""
Deprecated. ibm_db has no support for pyinstaller_executables 
Please check the supported jaydebeapi connection details in below scripts for connection to db2 via python
https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/src/remote/python/connect_jaydebeapi.py
https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/src/remote/python/job_monitoring_functions_jaydebeapi.py
(or)
Most recent BDD framework tested 
https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/behave-testing/steps/python_scripts/jc_start.py
https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/behave-testing/steps/python_scripts/job_monitoring_functions.py

this program establishes connection to remote db2 db01 database (using ibm_db clinet)

usage: python connect.py

Author: krishna damarla

Support: https://github.com/ibmdb/python-ibmdb
https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.swg.im.dbclient.python.doc/doc/c0054699.html
ibm_db python client material, fetch rows from db2 techniques, sending python variable inputs to sql code 
https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.swg.im.dbclient.python.doc/doc/c0054699.html
https://www.ibm.com/support/knowledgecenter/hu/SSEPGG_9.7.0/com.ibm.swg.im.dbclient.python.doc/doc/t0054388.html
https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.swg.im.dbclient.python.doc/doc/t0054696.html
"""

import ibm_db as db2

def make_connection(database, hostname, port, username, password, timeout):
        return db2.connect('DATABASE={0};'.format(database) +
                  'HOSTNAME={0};'.format(hostname) +
                  'PORT={0};'.format(str(port)) +
                  'PROTOCOL=TCPIP;' +
                  'UID={0};'.format(username) +
                  'PWD={0};'.format(password) +
                  'ConnectTimeout={0};'.format(str(timeout))
                  ,'','')

# connection credentials


def connect_db2():
        return make_connection('db01', '10.85.200.175', '50000', 'dbuser1', 'Xbv7J6A8RdqkP8mV', '30')

def connect_db02():
	return make_connection('db01', '10.85.200.180', '50000', 'db2inst1', '3Hk3fVFQkd3LFCzc', '30')


#print(con)
#ibm_db.connect("HOSTNAME=10.85.200.175;PORT=50000;PROTOCOL=TCPIP;DATABASE=db01;UID=db2inst1;PWD={3}#|/6kXTLBnV59", "","")


#print(connect_db2())
# create samp table

#con = connect_db2()

#stmt = db2.exec_immediate(con, "create table samp5 (sample integer not null)")

#print(stmt)
