"""
Connect to hive using beeline

usage: python hive_clientAPI_connection.py

Author: krishna damarla
"""
import subprocess

cmd = 'beeline -u "jdbc:hive2://10.85.52.14:10000/" -e "SELECT * FROM raw_vault.jurlv LIMIT 1;"'

status, output = subprocess.getstatusoutput(cmd)

if status == 0:
   print(output)
else:
   print("error")
