
from job_monitoring_functions import *
#from pyspark.sql import SparkSession, SQLContext, HiveContext
import numpy
import datetime
#import ibm_db as db2
import jaydebeapi as db2


#spark = SparkSession.builder.enableHiveSupport().getOrCreate()

con = db2.connect('com.ibm.db2.jcc.DB2Driver', 'jdbc:db2://10.85.200.175:50000/db01', ['dbuser1', 'Xbv7J6A8RdqkP8mV'], './db2jcc4.jar')

#con = connect_db2()

#create job execution table
curs = con.cursor()

# get pid grom getprocessid within job_monitoring functions !! 
pid = get_process_id("dwh",curs)

set_status(pid, 'finished',curs)

curs.close()

con.close()
