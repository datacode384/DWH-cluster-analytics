# Uploading properties & xml file to IWS (IBM Workload Scheduler) 

- submit a sample xml and [properties file](https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/src/remote/python/JS_ETL_NEU_Mapping.UTF8.properties) zipped into JS_ETL_NEU and move this zipped folsder to below iws insatlled machine from 10.85.52.14

job submission to iws scheduler as shown in below ss.( IWS is called previoulsy TWS (Tivoli workload scheduler) )
![job submission to iws](/Docs/IWS/job%20submission%20to%20iws.png)

This db_extraction script https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/src/remote/python/db_extraction.py gets all the scripts in the format what xml file suggests to targer folder here https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/tree/master/src/remote/python/target

The script to flatten xml file to text format (https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/src/remote/python/JS_ETL_NEU_Mapping.UTF8.properties) is https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/src/remote/python/job_properties.py

- The mock scripts are in /root/mock_scripts folder of 10.85.86.194 machine. 

# Issue:
- What is the gui url of tws scheduler to see the process after our submission for errors / blockers ? , we are not sure how to capture the error or if anything that stopped the scheduler. For example, we changed the properties file to some wrong setting and the import.sh file doesnot show any errors.  

