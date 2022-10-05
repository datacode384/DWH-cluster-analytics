"""
this program establishes connection to remote db2 db01 database and creates job_execution and job_step_execution tables

usage: python db2connect1.py

Author: krishna damarla
"""

import ibm_db as db2
from connect import connect_db2

con = connect_db2()

#create jobexecution table
job_exec_tbl = db2.exec_immediate(con, 
      """CREATE TABLE JOB_EXECUTION(
		  PROCESS_ID INTEGER NOT NULL , 
		  APPLICATION_NAME VARCHAR(100) NOT NULL , 
		  TENANT_NAME VARCHAR(100) NOT NULL , 
		  START_TST TIMESTAMP NOT NULL , 
		  END_TST TIMESTAMP , 
		  STATUS VARCHAR(100) NOT NULL, 
		  PRIMARY KEY("PROCESS_ID")""")
#print(job_exec_tbl)

# create jobstepexecution table
job_step_exec_tbl = db2.exec_immediate(con, 
      """CREATE TABLE JOB_STEP_EXECUTION(
		  PROCESS_STEP_ID INTEGER NOT NULL , 
		  PROCESS_ID INTEGER NOT NULL , 
		  STEP_NAME VARCHAR(100) NOT NULL , 
		  STEP_START_TST TIMESTAMP NOT NULL , 
		  STEP_END_TST TIMESTAMP , 
		  STEP_STATUS VARCHAR(100) NOT NULL , 
		  READ_COUNT INTEGER , 
		  FILTER_COUNT INTEGER , 
		  WRITE_COUNT INTEGER , 
		  ERROR_CODE VARCHAR(100) , 
		  RETURN_CODE INTEGER NOT NULL,
		  foreign key(process_id) 
		  references JOB_EXECUTION(process_id))""")

#jobstepexecution table alter key definitions
#ALTER TABLE DBUSER1.JOB_STEP_EXECUTION ADD CONSTRAINT JOB_STEP_EXECUTION_UN UNIQUE (STEP_NAME,PROCESS_ID);
#ALTER TABLE DBUSER1.JOB_STEP_EXECUTION ADD CONSTRAINT JOB_STEP_EXECUTION_PK PRIMARY KEY (PROCESS_STEP_ID);

# INSERT SOME VALUES TO jobstepexecution
"""
insert into JOB_STEP_EXECUTION(process_step_id, process_id, step_name, step_start_tst, step_end_tst, step_status, ERROR_CODE, table_name)
values(1, 1, 'lv_ingest', '2008-12-25-17.12.30.000000' , NULL, 'started', NULL,'lv')

insert into JOB_STEP_EXECUTION(process_step_id, process_id, step_name, step_start_tst, step_end_tst, step_status, ERROR_CODE, table_name)
values(2, 1, 'lv_load_rawvault', '2008-12-25-17.12.30.000000' , NULL, 'started', NULL,'lv')

insert into JOB_STEP_EXECUTION(process_step_id, process_id, step_name, step_start_tst, step_end_tst, step_status, ERROR_CODE, table_name)
values(3, 1, 'lv_load_core', '2008-12-25-17.12.30.000000' , NULL, 'started', NULL,'lv')

insert into JOB_STEP_EXECUTION(process_step_id, process_id, step_name, step_start_tst, step_end_tst, step_status, ERROR_CODE, table_name)
values(4, 1, 'vt_ingest', '2008-12-25-17.12.30.000000' , NULL, 'started', NULL,'vt')

insert into JOB_STEP_EXECUTION(process_step_id, process_id, step_name, step_start_tst, step_end_tst, step_status, ERROR_CODE, table_name)
values(5, 1, 'vt_load_rawvault', '2008-12-25-17.12.30.000000' , NULL, 'started', NULL,'vt')

insert into JOB_STEP_EXECUTION(process_step_id, process_id, step_name, step_start_tst, step_end_tst, step_status, ERROR_CODE, table_name)
values(6, 1, 'vt_load_core', '2008-12-25-17.12.30.000000' , NULL, 'started', NULL,'vt')
"""
# Reorg after alter
#call ADMIN_CMD('REORG TABLE JOB_EXECUTION');
