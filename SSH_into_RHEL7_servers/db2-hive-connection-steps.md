# Dbeaver client usage instructions to connect to any relational / hadoop databases 

## To connect to db2 database 

1. Get username / password /db name/ hostname credentials to connect to any realtional / hadoop databases

2. Download db2 driver from dbeaver and test connection. For example, to connect to db2, we need db2jcc4.jar. 

3. To connect to hive/impala, we need hive / impala jdbc drivers. 

4. If drivers are nor readily avaialble from dbeaver, download them from their respective websites and add downloaded driver files in dbeaver.

![Edit-db2-Connection](https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/Docs/screenshots/db2.png)

# ssh login to databases
- To login to a db2 database	
 - ssh db-machine-ip-address
 - enter openVPN password
 - sudo su -
 - cd /home
 - su db2inst1 (requires no password to connect to db2)
 - su dbuser1 (requires password when connecting with dbeaver client)
 - db2

Note: If db2 profile is not found for any user account, export . /home/db2inst1/sqllib/db2profile to user specific ~/.bashrc

## Ref:
https://github.com/ca-cwds/intake/wiki/Install-DBeaver-and-Connect-to-PreInt-DB2 
