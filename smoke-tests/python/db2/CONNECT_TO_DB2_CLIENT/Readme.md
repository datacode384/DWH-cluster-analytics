- To execute sql files in this folder as ```CONNECT TO 10.85.200.180:50000/db01 USER db2inst1 USING 3Hk3fVFQkd3LFCz``` we need either one of the following requirement to be met.
1. Need ssh access on db2 server to be able to login from cloudera node to db2 to run sql files directly on db2 (ensure the database where we want to create / insert into tables 
been cataloged Ex:  https://stackoverflow.com/a/12972748/5757129  
2. Install ibm db2 client in the machine where u have sql scripts. (Bit cumbersome than 1st method as this needs installion in every env we test smoke-test-pacakge) 
