"""
Connect to impala installed datanodes as shown below using python client impyla 

usage: python impala_clientAPI_connection.py

source : https://github.com/cloudera/impyla

Author: krishna damarla

"""

from impala.dbapi import connect

conn = connect(host = 'dwh-hdp-node02.dev.ergo.liferunoffinsuranceplatform.com', port=21050)

#conn = connect(host = 'dwh-hdp-node03.dev.ergo.liferunoffinsuranceplatform.com', port=21050)

cursor = conn.cursor()

cursor.execute('SHOW DATABASES')

res= cursor.fetchall()

for data in res:
        print(data)
        

