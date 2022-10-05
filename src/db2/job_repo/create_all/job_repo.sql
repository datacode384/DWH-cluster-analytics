-- This CLP file was created using DB2LOOK Version "11.1" 
-- Timestamp: Sat 01 Aug 2020 02:00:05 AM CEST
-- Database Name: DB01           
-- Database Manager Version: DB2/LINUXX8664 Version 11.1.2.2
-- Database Codepage: 1208
-- Database Collating Sequence is: IDENTITY
-- Alternate collating sequence(alt_collate): null
-- varchar2 compatibility(varchar2_compat): OFF


CONNECT TO DB01;

------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."ENVIRONMENT"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."ENVIRONMENT"  (
		  "ENVIRONMENT_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "ENVIRONMENT_NAME" VARCHAR(10 OCTETS) NOT NULL )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."ENVIRONMENT" IS 'Reference table that defines stages  avaialble in system.  For ex, production DEV. ';


-- DDL Statements for Primary Key on Table "JOB_REPO"."ENVIRONMENT"

ALTER TABLE "JOB_REPO"."ENVIRONMENT" 
	ADD CONSTRAINT "ENVIRONMENT_PK" PRIMARY KEY
		("ENVIRONMENT_ID");



------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."PARAMETER"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."PARAMETER"  (
		  "PARAMETER_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "PARAMETER_NAME" VARCHAR(50 OCTETS) NOT NULL , 
		  "DESCRIPTION" VARCHAR(500 OCTETS) NOT NULL , 
		  "JOB_CONTROL_SORT" INTEGER , 
		  "IWS_NODE" VARCHAR(100 OCTETS) , 
		  "IWS_ATTRIBUTE" VARCHAR(100 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."PARAMETER" IS 'Reference table that defines all parameters used in job_group_parameter, job_paramter, job_chain_paramter  for generating jobs.';


-- DDL Statements for Primary Key on Table "JOB_REPO"."PARAMETER"

ALTER TABLE "JOB_REPO"."PARAMETER" 
	ADD CONSTRAINT "PARAMETER_PK" PRIMARY KEY
		("PARAMETER_ID");



------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."PARAMETER_GROUP"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."PARAMETER_GROUP"  (
		  "PARAMETER_GROUP_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "PARAMETER_NAME" VARCHAR(50 OCTETS) , 
		  "DESCRIPTION" VARCHAR(512 OCTETS) , 
		  "DATABASE" VARCHAR(200 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."PARAMETER_GROUP" IS 'Reference table that defines paramter groups used in table job and table parameter_group_value';


-- DDL Statements for Primary Key on Table "JOB_REPO"."PARAMETER_GROUP"

ALTER TABLE "JOB_REPO"."PARAMETER_GROUP" 
	ADD CONSTRAINT "PARAMETER_GROUP_PK" PRIMARY KEY
		("PARAMETER_GROUP_ID");



------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."PARAMETER_GROUP_VALUE"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."PARAMETER_GROUP_VALUE"  (
		  "PARAMETER_GROUP_VALUE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "PARAMETER_GROUP_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "VALUE" VARCHAR(512 OCTETS) , 
		  "LINK_REF" VARCHAR(100 OCTETS) , 
		  "STRING_FLAG" BOOLEAN WITH DEFAULT TRUE , 
		  "FILTER_CONDITION" VARCHAR(2000 OCTETS) , 
		  "SELECTION_FIELD" VARCHAR(2000 OCTETS) , 
		  "REFERENCE_FIELD_TARGET" VARCHAR(2000 OCTETS) , 
		  "FIELD_TRANSFORMATION" VARCHAR(2000 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."PARAMETER_GROUP_VALUE" IS 'Contains set of values for each parameter group For ex: all tables of a paramter group b. ';





-- DDL Statements for Unique Constraints on Table "JOB_REPO"."PARAMETER_GROUP_VALUE"


ALTER TABLE "JOB_REPO"."PARAMETER_GROUP_VALUE" 
	ADD CONSTRAINT "PARAMETER_GROUP_VALUE_PK" UNIQUE
		("PARAMETER_GROUP_VALUE_ID",
		 "PARAMETER_GROUP_ID");


------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."SCRIPT"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."SCRIPT"  (
		  "SCRIPT_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "NAME" VARCHAR(100 OCTETS) , 
		  "SOURCE_CODE" VARCHAR(32000 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."SCRIPT" IS 'Code script template for core tables with differnet paramters. These parameter values are set in v_job_paramter and v_generated_scripts. ';


-- DDL Statements for Primary Key on Table "JOB_REPO"."SCRIPT"

ALTER TABLE "JOB_REPO"."SCRIPT" 
	ADD CONSTRAINT "SCRIPT_PK" PRIMARY KEY
		("SCRIPT_ID");



------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."JOB"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."JOB"  (
		  "JOB_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "JOB_NAME" VARCHAR(50 OCTETS) , 
		  "JOB_GROUP_ID" VARCHAR(100 OCTETS) , 
		  "PARAMETER_GROUP_ID" VARCHAR(100 OCTETS) , 
		  "DESCRIPTION" VARCHAR(512 OCTETS) , 
		  "SCRIPT_ID" VARCHAR(50 OCTETS) , 
		  "DATABASE" VARCHAR(100 OCTETS) , 
		  "TABLE" VARCHAR(100 OCTETS) , 
		  "GENERATE_KEY_FLAG" BOOLEAN NOT NULL WITH DEFAULT FALSE , 
		  "OVERWRITE_SCRIPT_ID" VARCHAR(200 OCTETS) WITH DEFAULT NULL , 
		  "STARTER_SCRIPT_ID" VARCHAR(100 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."JOB" IS 'Defines jobs that are executed by IWS. 
For every parameter_group_id, we linnk corresponing parameter_group_value_id . For example, the result concentation of parameter_group_id: lv_core  is 
 lv_core_vt, lv_core_hvgruppe etc ';


-- DDL Statements for Primary Key on Table "JOB_REPO"."JOB"

ALTER TABLE "JOB_REPO"."JOB" 
	ADD CONSTRAINT "JOB_PK" PRIMARY KEY
		("JOB_ID");



------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."JOB_CHAIN"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."JOB_CHAIN"  (
		  "JOB_CHAIN_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "JOB_CHAIN_NAME" VARCHAR(100 OCTETS) NOT NULL , 
		  "DESCRIPTION" VARCHAR(512 OCTETS) , 
		  "START_JOB_ID" VARCHAR(100 OCTETS) , 
		  "END_JOB_ID" VARCHAR(100 OCTETS) , 
		  "ENVIRONMENT_ID" VARCHAR(200 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."JOB_CHAIN" IS 'Defines attributes(jc_start, jc_stop) of the job_chain. A job_chain gets all the jobs to load to raw_zone / core. Currently we have only one job_chain PROD. ';


-- DDL Statements for Primary Key on Table "JOB_REPO"."JOB_CHAIN"

ALTER TABLE "JOB_REPO"."JOB_CHAIN" 
	ADD CONSTRAINT "JOB_CHAIN_PK" PRIMARY KEY
		("JOB_CHAIN_ID");



------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."JOB_CHAIN_PARAMETER"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."JOB_CHAIN_PARAMETER"  (
		  "JOB_CHAIN_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "PARAMETER_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "VALUE" VARCHAR(512 OCTETS) NOT NULL , 
		  "ENVIRONMENT_ID" VARCHAR(100 OCTETS) NOT NULL )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."JOB_CHAIN_PARAMETER" IS 'Defines parameters for all jobs within the job_chain. Ex: current 13 parameters in the data';





-- DDL Statements for Unique Constraints on Table "JOB_REPO"."JOB_CHAIN_PARAMETER"


ALTER TABLE "JOB_REPO"."JOB_CHAIN_PARAMETER" 
	ADD CONSTRAINT "JOB_CHAIN_PARAMETER_PK" UNIQUE
		("JOB_CHAIN_ID",
		 "PARAMETER_ID",
		 "ENVIRONMENT_ID");


------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."JOB_DEPENDENCY"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."JOB_DEPENDENCY"  (
		  "PARENT_JOB_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "CHILD_JOB_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "PARENT_PARAMETER_GROUP_VALUE_ID" VARCHAR(100 OCTETS) , 
		  "CHILD_PARAMETER_GROUP_VALUE_ID" VARCHAR(100 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."JOB_DEPENDENCY" IS 'Defines dependencies between jobs. Child jobs are executed if all parent jobs have been finished successfully. Ex: filter child_id with jc_stop. U get 5 parent jobs. ';






------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."JOB_PARAMETER"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."JOB_PARAMETER"  (
		  "JOB_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "PARAMETER_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "VALUE" VARCHAR(512 OCTETS) , 
		  "STRING_FLAG" BOOLEAN NOT NULL WITH DEFAULT true )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."JOB_PARAMETER" IS 'Deines parameter values specific to a job within the job group. ';





-- DDL Statements for Unique Constraints on Table "JOB_REPO"."JOB_PARAMETER"


ALTER TABLE "JOB_REPO"."JOB_PARAMETER" 
	ADD CONSTRAINT "JOB_PARAMETER_PK" UNIQUE
		("JOB_ID",
		 "PARAMETER_ID");


------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."TENANT"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."TENANT"  (
		  "TENANT_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "TENANT_NAME" VARCHAR(100 OCTETS) NOT NULL WITH DEFAULT '' , 
		  "DESCRIPTION" VARCHAR(500 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."TENANT" IS 'Reference table for tenants';


-- DDL Statements for Primary Key on Table "JOB_REPO"."TENANT"

ALTER TABLE "JOB_REPO"."TENANT" 
	ADD CONSTRAINT "TENANT_PK" PRIMARY KEY
		("TENANT_ID");



------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."ENVIRONMENT_TENANT"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."ENVIRONMENT_TENANT"  (
		  "ENVIRONMENT_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "TENANT_ID" VARCHAR(100 OCTETS) NOT NULL )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."ENVIRONMENT_TENANT" IS 'Defines which tenants(itergo, or other test tenants) are used in environment(dev/sit/cit). Test to be modified. ';





-- DDL Statements for Unique Constraints on Table "JOB_REPO"."ENVIRONMENT_TENANT"


ALTER TABLE "JOB_REPO"."ENVIRONMENT_TENANT" 
	ADD CONSTRAINT "ENVIRONMENT_TENANT_PK" UNIQUE
		("ENVIRONMENT_ID",
		 "TENANT_ID");


------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."JOB_GROUP"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."JOB_GROUP"  (
		  "JOB_GROUP_ID" VARCHAR(200 OCTETS) NOT NULL , 
		  "NAME" VARCHAR(200 OCTETS) NOT NULL )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."JOB_GROUP" IS 'This table is used for gropping jobs . Ex: Allj obs that are used for loading data to core datbase belong to one job group. ';


-- DDL Statements for Indexes on Table "JOB_REPO"."JOB_GROUP"

SET SYSIBM.NLS_STRING_UNITS = 'SYSTEM';

CREATE UNIQUE INDEX "JOB_REPO"."JOB_GROUP_JOB_GROUP_ID_UINDEX" ON "JOB_REPO"."JOB_GROUP" 
		("JOB_GROUP_ID" ASC)
		
		COMPRESS NO 
		INCLUDE NULL KEYS ALLOW REVERSE SCANS;
-- DDL Statements for Primary Key on Table "JOB_REPO"."JOB_GROUP"

ALTER TABLE "JOB_REPO"."JOB_GROUP" 
	ADD CONSTRAINT "JOB_GROUP_PK" PRIMARY KEY
		("JOB_GROUP_ID");




------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."JOB_GROUP_PARAMETER"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."JOB_GROUP_PARAMETER"  (
		  "JOB_GROUP_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "PARAMETER_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "VALUE" VARCHAR(200 OCTETS) NOT NULL )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."JOB_GROUP_PARAMETER" IS 'Defines  parameters and their values avaialble for all jobs within job groups. For example, to add table prefix pas, par, we get extension_type values as pas in data (current vaule is cp) ';






------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."DATABASE_PARAMETER"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."DATABASE_PARAMETER"  (
		  "ENVIRONMENT_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "TENANT_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "DATABASE" VARCHAR(100 OCTETS) NOT NULL , 
		  "PARAMETER" VARCHAR(100 OCTETS) NOT NULL )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "JOB_REPO"."DATABASE_PARAMETER" IS 'Defines Hive databases for each tenant (itergo). This table is obsolete and have to be replaced. Need some modifications. ';






------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."TENANT_PARAMETER"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."TENANT_PARAMETER"  (
		  "ENVIRONMENT_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "TENANT_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "VALUE" VARCHAR(1000 OCTETS) NOT NULL , 
		  "PARAMETER" VARCHAR(100 OCTETS) NOT NULL )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 






------------------------------------------------
-- DDL Statements for Table "JOB_REPO"."STARTER_SCRIPT"
------------------------------------------------
 

CREATE TABLE "JOB_REPO"."STARTER_SCRIPT"  (
		  "STARTER_SCRIPT_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "DESCRIPTION" VARCHAR(1000 OCTETS) , 
		  "SOURCE_CODE" VARCHAR(2000 OCTETS) NOT NULL )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 






-- DDL Statements for Foreign Keys on Table "JOB_REPO"."PARAMETER_GROUP_VALUE"

ALTER TABLE "JOB_REPO"."PARAMETER_GROUP_VALUE" 
	ADD CONSTRAINT "PARAMETER_GROUP_VALUE_PARAMETER_GROUP_PARAMETER_GROUP_ID_FK" FOREIGN KEY
		("PARAMETER_GROUP_ID")
	REFERENCES "JOB_REPO"."PARAMETER_GROUP"
		("PARAMETER_GROUP_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

-- DDL Statements for Foreign Keys on Table "JOB_REPO"."JOB"

ALTER TABLE "JOB_REPO"."JOB" 
	ADD CONSTRAINT "JOB_JOB_GROUP_JOB_GROUP_ID_FK" FOREIGN KEY
		("JOB_GROUP_ID")
	REFERENCES "JOB_REPO"."JOB_GROUP"
		("JOB_GROUP_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

ALTER TABLE "JOB_REPO"."JOB" 
	ADD CONSTRAINT "JOB_SCRIPT_SCRIPT_ID_FK" FOREIGN KEY
		("SCRIPT_ID")
	REFERENCES "JOB_REPO"."SCRIPT"
		("SCRIPT_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

-- DDL Statements for Foreign Keys on Table "JOB_REPO"."JOB_CHAIN"

ALTER TABLE "JOB_REPO"."JOB_CHAIN" 
	ADD CONSTRAINT "JOB_CHAIN_END_FK" FOREIGN KEY
		("END_JOB_ID")
	REFERENCES "JOB_REPO"."JOB"
		("JOB_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

ALTER TABLE "JOB_REPO"."JOB_CHAIN" 
	ADD CONSTRAINT "JOB_CHAIN__START_JOB_FK" FOREIGN KEY
		("START_JOB_ID")
	REFERENCES "JOB_REPO"."JOB"
		("JOB_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

-- DDL Statements for Foreign Keys on Table "JOB_REPO"."JOB_CHAIN_PARAMETER"

ALTER TABLE "JOB_REPO"."JOB_CHAIN_PARAMETER" 
	ADD CONSTRAINT "JOB_CHAIN_PARAMETER_ENVIRONMENT_ENVIRONMENT_ID_FK" FOREIGN KEY
		("ENVIRONMENT_ID")
	REFERENCES "JOB_REPO"."ENVIRONMENT"
		("ENVIRONMENT_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

ALTER TABLE "JOB_REPO"."JOB_CHAIN_PARAMETER" 
	ADD CONSTRAINT "JOB_CHAIN_PARAMETER_JC__FK" FOREIGN KEY
		("JOB_CHAIN_ID")
	REFERENCES "JOB_REPO"."JOB_CHAIN"
		("JOB_CHAIN_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

ALTER TABLE "JOB_REPO"."JOB_CHAIN_PARAMETER" 
	ADD CONSTRAINT "JOB_CHAIN_PARAMETER_PAR_FK" FOREIGN KEY
		("PARAMETER_ID")
	REFERENCES "JOB_REPO"."PARAMETER"
		("PARAMETER_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

-- DDL Statements for Foreign Keys on Table "JOB_REPO"."JOB_DEPENDENCY"

ALTER TABLE "JOB_REPO"."JOB_DEPENDENCY" 
	ADD CONSTRAINT "JOB_DEPENDENCY_JOB_JOB_ID_FK" FOREIGN KEY
		("PARENT_JOB_ID")
	REFERENCES "JOB_REPO"."JOB"
		("JOB_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

ALTER TABLE "JOB_REPO"."JOB_DEPENDENCY" 
	ADD CONSTRAINT "JOB_DEPENDENCY___FK2" FOREIGN KEY
		("CHILD_JOB_ID")
	REFERENCES "JOB_REPO"."JOB"
		("JOB_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

-- DDL Statements for Foreign Keys on Table "JOB_REPO"."JOB_PARAMETER"

ALTER TABLE "JOB_REPO"."JOB_PARAMETER" 
	ADD CONSTRAINT "JOB_PARAMETER_JOB_FK" FOREIGN KEY
		("JOB_ID")
	REFERENCES "JOB_REPO"."JOB"
		("JOB_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

ALTER TABLE "JOB_REPO"."JOB_PARAMETER" 
	ADD CONSTRAINT "JOB_PARAMETER_PARAMETER_PARAMETER_ID_FK" FOREIGN KEY
		("PARAMETER_ID")
	REFERENCES "JOB_REPO"."PARAMETER"
		("PARAMETER_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

-- DDL Statements for Foreign Keys on Table "JOB_REPO"."ENVIRONMENT_TENANT"

ALTER TABLE "JOB_REPO"."ENVIRONMENT_TENANT" 
	ADD CONSTRAINT "ENVIRONMENT_TENANT_ENV__FK" FOREIGN KEY
		("ENVIRONMENT_ID")
	REFERENCES "JOB_REPO"."ENVIRONMENT"
		("ENVIRONMENT_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

ALTER TABLE "JOB_REPO"."ENVIRONMENT_TENANT" 
	ADD CONSTRAINT "ENVIRONMENT_TENANT_TENANT_TENANT_ID_FK" FOREIGN KEY
		("TENANT_ID")
	REFERENCES "JOB_REPO"."TENANT"
		("TENANT_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

-- DDL Statements for Foreign Keys on Table "JOB_REPO"."JOB_GROUP_PARAMETER"

ALTER TABLE "JOB_REPO"."JOB_GROUP_PARAMETER" 
	ADD CONSTRAINT "JOB_GROUP_PARAMETER_FK" FOREIGN KEY
		("JOB_GROUP_ID")
	REFERENCES "JOB_REPO"."JOB_GROUP"
		("JOB_GROUP_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

ALTER TABLE "JOB_REPO"."JOB_GROUP_PARAMETER" 
	ADD CONSTRAINT "JOB_GROUP_PARAMETER_PARAMETER_PARAMETER_ID_FK" FOREIGN KEY
		("PARAMETER_ID")
	REFERENCES "JOB_REPO"."PARAMETER"
		("PARAMETER_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

-- DDL Statements for Foreign Keys on Table "JOB_REPO"."DATABASE_PARAMETER"

ALTER TABLE "JOB_REPO"."DATABASE_PARAMETER" 
	ADD CONSTRAINT "DATABASE_PARAMETER_ENVIRONMENT_ENVIRONMENT_ID_FK" FOREIGN KEY
		("ENVIRONMENT_ID")
	REFERENCES "JOB_REPO"."ENVIRONMENT"
		("ENVIRONMENT_ID")
	ON DELETE CASCADE
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

ALTER TABLE "JOB_REPO"."DATABASE_PARAMETER" 
	ADD CONSTRAINT "DATABASE_PARAMETER_TENANT_TENANT_ID_FK" FOREIGN KEY
		("TENANT_ID")
	REFERENCES "JOB_REPO"."TENANT"
		("TENANT_ID")
	ON DELETE CASCADE
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;






----------------------------

-- DDL Statements for Views

----------------------------
SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view JOB_REPO.V_JOB_DEPENDENCIES (PARENT_JOB_NAME, child_JOB_NAME)
AS
    SELECT src.JOB_NAME PARENT_JOB_NAME, tgt.job_name CHILD_JOB_NAME
    FROM JOB_REPO.V_TEC_JOBS src
             join JOB_REPO.JOB_DEPENDENCY jd on (jd.PARENT_JOB_ID = src.JOB_ID)
             join JOB_REPO.V_TEC_JOBS tgt on (jd.CHILD_JOB_ID = tgt.JOB_ID and tgt.VALUE = src.value)
    where jd.PARENT_PARAMETER_GROUP_VALUE_ID is null
      and jd.CHILD_PARAMETER_GROUP_VALUE_ID is null
    union
    SELECT src.JOB_NAME PARENT_JOB_NAME, tgt.job_name
    FROM JOB_REPO.V_TEC_JOBS src
             join JOB_REPO.JOB_DEPENDENCY jd on (jd.PARENT_JOB_ID = src.JOB_ID)
             join JOB_REPO.V_TEC_JOBS tgt on (jd.CHILD_JOB_ID = tgt.JOB_ID)
    where tgt.VALUE is null
      and src.VALUE is not null
      and jd.PARENT_PARAMETER_GROUP_VALUE_ID is null
      and jd.CHILD_PARAMETER_GROUP_VALUE_ID is null
    union
    SELECT src.JOB_NAME PARENT_JOB_NAME, tgt.job_name
    FROM JOB_REPO.V_TEC_JOBS src
             join JOB_REPO.JOB_DEPENDENCY jd on (jd.PARENT_JOB_ID = src.JOB_ID)
             join  JOB_REPO.V_TEC_JOBS tgt on (jd.CHILD_JOB_ID = tgt.JOB_ID)
    where tgt.VALUE is not null
      and src.VALUE is null
      and jd.PARENT_PARAMETER_GROUP_VALUE_ID is null
      and jd.CHILD_PARAMETER_GROUP_VALUE_ID is null
    union
    SELECT src.JOB_NAME PARENT_JOB_NAME, tgt.job_name
    FROM JOB_REPO.V_TEC_JOBS src
             join JOB_REPO.JOB_DEPENDENCY jd on (jd.PARENT_JOB_ID = src.JOB_ID)
             join JOB_REPO.V_TEC_JOBS tgt on (jd.CHILD_JOB_ID = tgt.JOB_ID)
    where tgt.VALUE is null
      and src.VALUE is null
      and jd.PARENT_PARAMETER_GROUP_VALUE_ID is null
      and jd.CHILD_PARAMETER_GROUP_VALUE_ID is null
    UNION
    SELECT src.JOB_NAME PARENT_JOB_NAME, tgt.JOB_NAME
    FROM JOB_REPO.V_TEC_JOBS srC
             join JOB_REPO.JOB_DEPENDENCY jd on (jd.PARENT_JOB_ID = src.JOB_ID)
             join JOB_REPO.V_TEC_JOBS tgt on (jd.CHILD_JOB_ID = tgt.JOB_ID)
    where tgt.VALUE is null
      and src.VALUE is not null
      and jd.parent_PARAMETER_GROUP_VALUE_ID is not null
      and jd.CHILD_PARAMETER_GROUP_VALUE_ID is null
      and jd.parent_PARAMETER_GROUP_VALUE_ID = src.PARAMETER_GROUP_VALUE_ID;


COMMENT ON TABLE "JOB_REPO"."V_JOB_DEPENDENCIES" IS ' Adds the table name from the job_dependency table. Child job starts after finishing parent job. Ex: Core Integration(CI) starts after Rawvault Integration(RI) ';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_job_dependencies_final AS
SELECT distinct v.Tenant_name,
       tenant_name || '_' || a.CHILD_JOB_NAME CHILD_JOB_NAME,
       tenant_name || '_' || a.PARENT_JOB_NAME PARENT_JOB_NAME
FROM V_JOB_DEPENDENCIES a
         CROSS JOIN V_ENVIRONMENT_TENANT v;


COMMENT ON TABLE "JOB_REPO"."V_JOB_DEPENDENCIES_FINAL" IS ' Adds the Tenant name(itergo) to V_JOB_DEPENDECIES. Child job starts after finishing parent job. Ex: Core Integration(CI) starts after Rawvault Integration(RI) ';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view v_iws_docu_job_par as
with dec as (
    select ENVIRONMENT_NAME, e.TENANT_NAME, upper( JOB_NAME) JOB_NAME, parameter_name, value
    from V_JOB_PARAMETER
    cross join V_ENVIRONMENT_TENANT e
    where parameter_name LIKE 'JC%'
    and e.ENVIRONMENT_ID like 'PROD'
),
     exec as (
         select a.job_name, 'model:task' as element ,'exec' as attribut, a.value || TENANT_NAME || '/' || lower( a.job_name) || '.sh' value
         from dec a
         where a.parameter_name = 'JC_TASK_PATH'
     ),
     recovery as (
         select a.job_name, 'model:recovery' as element ,'action' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_REC_ACTION'
     ),
     rerun as (
         select a.job_name, 'model:recovery' as element ,'abendPrompt' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_REC_ABEND_PROMPT'
     ),
     logon as (
         select a.job_name, 'model:JobDefinition' as element ,'logon' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_LOGON'
     ),
     folder as (
         select a.job_name, 'model:JobDefinition' as element, 'folder' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     workstation as (
         select a.job_name, 'model:JobDefinition' as element, 'workstation' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_WORKSTATION'
     ),
     name as (
         select a.job_name, 'model:JobDefinition' as element, 'name' as attribut, '$' || a.value || '_' ||  a.job_name || '$' value
         from dec a
         where a.parameter_name = 'JC_JD_JOB_PREFIX'
     ),
     js_name as (
         select a.job_name, 'model:JobStream' as element, 'name' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JS_NAME'
     ),
     js_priority as (
         select a.job_name, 'model:JobStream' as element, 'priority' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JOB_PRIORITY'
     ),
     js_folder as (
         select a.job_name, 'model:JobStream' as element, 'folder' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_confirmed as (
         select a.job_name, 'model:job' as element, 'confirmed' as attribut,  'false' value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_iscritical as (
         select a.job_name, 'model:job' as element, 'iscritical' as attribut,  'false'  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_key as (
         select a.job_name, 'model:job' as element, 'iskey' as attribut,   'false'  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
    jd_name as (
         select a.job_name, 'model:job' as element, 'name' as attribut, '$' || a.value || '_' ||  a.job_name || '$' value
         from dec a
         where a.parameter_name = 'JC_JD_JOB_PREFIX'
     ),
     jd_priority as (
         select a.job_name, 'model:job' as element, 'priority' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JOB_PRIORITY'
     ),
     jd_definition as (
         select a.job_name, 'model:job' as element, 'definition' as attribut, a.value || '#' || b.value  || '#' || n.value value
         from dec a
         join dec b on (a.JOB_NAME = b.JOB_NAME and b.parameter_name = 'JC_JD_FOLDER')
         join name n on (a.JOB_NAME = n.JOB_NAME)
         where a.parameter_name = 'JC_JD_WORKSTATION'

     ),
     jd_pred as(
          select distinct a.job_name, 'model:predecessor' as element, 'target' as attribut, b.PARENT_JOB_NAME value
         from dec a
         join V_JOB_DEPENDENCIES b on (lower(b.CHILD_JOB_NAME) = lower(a.JOB_NAME))

     ),
     jd_succ as(
        select distinct a.job_name, 'model:status' as element, 'name' as attribut,   'SUCC'  value
         from  jd_pred a
     ),
     all_par as (
select *
from exec
union all
select *
from logon
union all
select *
from folder
union all
select *
from workstation
    union all
select *
from recovery
union all
select *
from rerun
union all
select *
from name
union all
select *
from js_name
union all
select *
from js_priority
union all
select *
from js_folder
union all
select *
from jd_confirmed
union all
select *
from jd_iscritical
union all
select *
from jd_key
union all
select *
from jd_name
union all
select *
from jd_priority
union all
select *
from jd_definition
union all
select *
from  jd_pred
    union all
select *
from
          jd_succ
         )

select * from all_par;



SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE 
VIEW v_iws_job_chain_tenant AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


COMMENT ON TABLE "JOB_REPO"."V_IWS_JOB_CHAIN_TENANT" IS 'Used for documentation purposes to store all job names ';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_tenant_neu AS   WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tpa2.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    upper(tpa.value)  "WRAPPER_SCRIPT"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
Left join tenant_parameter tpa ON(
    tpa.tenant_id =  et.tenant_id and tpa.PARAMETER = 'WRAPPER_SCRIPT'
        )
Left join tenant_parameter tpa2 ON(
    tpa2.tenant_id =  et.tenant_id and tpa2.PARAMETER = 'JC_TASK_PATH'
        )
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || lower(nvl(wrapper_script,job_name)) || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ) ) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final_test AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ),
    XMLELEMENT(NAME "model:Resource",
    XMLATTRIBUTES(MIN(JC_RES_DB2_DEFAULT_VALUE) AS "name",
    '2' AS "units",
    MIN(JC_JD_WORKSTATION) AS "workstation"))) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;








SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW JOB_REPO.v_environment_tenant AS
SELECT
    pE.PARAMETER_NAME PARAMTER_ENVIRONMENT_NAME,
    e.environment_name,
    e.environment_id,
    pe.PARAMETER_ID PARAMTER_ENVIRONMENT_ID,
    pt.parameter_name PARAMETER_TENANT_NAME,
    t.TENANT_NAME,
    t.TENANT_ID,
    pt.PARAMETER_ID parameter_tenant_id
FROM
    JOB_REPO.environment e
JOIN JOB_REPO.ENVIRONMENT_TENANT et ON
    (e.ENVIRONMENT_ID = et.ENVIRONMENT_ID)
JOIN JOB_REPO.TENANT t ON
    (t.TENANT_ID = et.TENANT_ID)
CROSS JOIN JOB_REPO.PARAMETER pt
CROSS JOIN JOB_REPO.PARAMETER pe
WHERE
    pt.PARAMETER_ID = 'TENANT'
    AND pe.PARAMETER_ID = 'ENVIRONMENT';


COMMENT ON TABLE "JOB_REPO"."V_ENVIRONMENT_TENANT" IS 'Joins table env & env_tenant. Provides parameteres used for creating wrapper scripts';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_job_dependencies_final AS
SELECT distinct v.Tenant_name,
       tenant_name || '_' || a.CHILD_JOB_NAME CHILD_JOB_NAME,
       tenant_name || '_' || a.PARENT_JOB_NAME PARENT_JOB_NAME
FROM V_JOB_DEPENDENCIES a
         CROSS JOIN V_ENVIRONMENT_TENANT v;


COMMENT ON TABLE "JOB_REPO"."V_JOB_DEPENDENCIES_FINAL" IS ' Adds the Tenant name(itergo) to V_JOB_DEPENDECIES. Child job starts after finishing parent job. Ex: Core Integration(CI) starts after Rawvault Integration(RI) ';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE 
VIEW v_iws_job_chain_tenant AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


COMMENT ON TABLE "JOB_REPO"."V_IWS_JOB_CHAIN_TENANT" IS 'Used for documentation purposes to store all job names ';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view v_iws_docu_job_par as
with dec as (
    select ENVIRONMENT_NAME, e.TENANT_NAME, upper( JOB_NAME) JOB_NAME, parameter_name, value
    from V_JOB_PARAMETER
    cross join V_ENVIRONMENT_TENANT e
    where parameter_name LIKE 'JC%'
    and e.ENVIRONMENT_ID like 'PROD'
),
     exec as (
         select a.job_name, 'model:task' as element ,'exec' as attribut, a.value || TENANT_NAME || '/' || lower( a.job_name) || '.sh' value
         from dec a
         where a.parameter_name = 'JC_TASK_PATH'
     ),
     recovery as (
         select a.job_name, 'model:recovery' as element ,'action' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_REC_ACTION'
     ),
     rerun as (
         select a.job_name, 'model:recovery' as element ,'abendPrompt' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_REC_ABEND_PROMPT'
     ),
     logon as (
         select a.job_name, 'model:JobDefinition' as element ,'logon' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_LOGON'
     ),
     folder as (
         select a.job_name, 'model:JobDefinition' as element, 'folder' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     workstation as (
         select a.job_name, 'model:JobDefinition' as element, 'workstation' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_WORKSTATION'
     ),
     name as (
         select a.job_name, 'model:JobDefinition' as element, 'name' as attribut, '$' || a.value || '_' ||  a.job_name || '$' value
         from dec a
         where a.parameter_name = 'JC_JD_JOB_PREFIX'
     ),
     js_name as (
         select a.job_name, 'model:JobStream' as element, 'name' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JS_NAME'
     ),
     js_priority as (
         select a.job_name, 'model:JobStream' as element, 'priority' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JOB_PRIORITY'
     ),
     js_folder as (
         select a.job_name, 'model:JobStream' as element, 'folder' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_confirmed as (
         select a.job_name, 'model:job' as element, 'confirmed' as attribut,  'false' value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_iscritical as (
         select a.job_name, 'model:job' as element, 'iscritical' as attribut,  'false'  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_key as (
         select a.job_name, 'model:job' as element, 'iskey' as attribut,   'false'  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
    jd_name as (
         select a.job_name, 'model:job' as element, 'name' as attribut, '$' || a.value || '_' ||  a.job_name || '$' value
         from dec a
         where a.parameter_name = 'JC_JD_JOB_PREFIX'
     ),
     jd_priority as (
         select a.job_name, 'model:job' as element, 'priority' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JOB_PRIORITY'
     ),
     jd_definition as (
         select a.job_name, 'model:job' as element, 'definition' as attribut, a.value || '#' || b.value  || '#' || n.value value
         from dec a
         join dec b on (a.JOB_NAME = b.JOB_NAME and b.parameter_name = 'JC_JD_FOLDER')
         join name n on (a.JOB_NAME = n.JOB_NAME)
         where a.parameter_name = 'JC_JD_WORKSTATION'

     ),
     jd_pred as(
          select distinct a.job_name, 'model:predecessor' as element, 'target' as attribut, b.PARENT_JOB_NAME value
         from dec a
         join V_JOB_DEPENDENCIES b on (lower(b.CHILD_JOB_NAME) = lower(a.JOB_NAME))

     ),
     jd_succ as(
        select distinct a.job_name, 'model:status' as element, 'name' as attribut,   'SUCC'  value
         from  jd_pred a
     ),
     all_par as (
select *
from exec
union all
select *
from logon
union all
select *
from folder
union all
select *
from workstation
    union all
select *
from recovery
union all
select *
from rerun
union all
select *
from name
union all
select *
from js_name
union all
select *
from js_priority
union all
select *
from js_folder
union all
select *
from jd_confirmed
union all
select *
from jd_iscritical
union all
select *
from jd_key
union all
select *
from jd_name
union all
select *
from jd_priority
union all
select *
from jd_definition
union all
select *
from  jd_pred
    union all
select *
from
          jd_succ
         )

select * from all_par;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_tenant_neu AS   WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tpa2.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    upper(tpa.value)  "WRAPPER_SCRIPT"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
Left join tenant_parameter tpa ON(
    tpa.tenant_id =  et.tenant_id and tpa.PARAMETER = 'WRAPPER_SCRIPT'
        )
Left join tenant_parameter tpa2 ON(
    tpa2.tenant_id =  et.tenant_id and tpa2.PARAMETER = 'JC_TASK_PATH'
        )
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || lower(nvl(wrapper_script,job_name)) || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ) ) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final_test AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ),
    XMLELEMENT(NAME "model:Resource",
    XMLATTRIBUTES(MIN(JC_RES_DB2_DEFAULT_VALUE) AS "name",
    '2' AS "units",
    MIN(JC_JD_WORKSTATION) AS "workstation"))) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view V_JOB_PARAMETER_ENV_TEN as
with q1 as (
select e.environment_name,
       e.tenant_name,
       e.tenant_name ||'_' || v.job_name job_name,
       e.PARAMTER_ENVIRONMENT_ID parameter_name,
       e.environment_name        value,
       1                         priority,
       v.SOURCE_CODE,
       v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e),
     q2 as(
select e.environment_name,
       e.tenant_name,
        e.tenant_name ||'_' || v.job_name job_name,
       e.PARAMETER_TENANT_ID parameter_name,
       e.TENANT_NAME         value,
       2                     priority,
       v.SOURCE_CODE,
         v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e),
     q3 as(
select e.environment_name,
       e.tenant_name,
        e.tenant_name ||'_' || v.job_name job_name,
       p.PARAMETER parameter_name,
       p.DATABASE,
       3 priority,
       v.SOURCE_CODE,
         v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e
        join JOB_REPO.database_parameter p on (p.ENVIRONMENT_ID = e.ENVIRONMENT_id and e.TENANT_ID = p.TENANT_ID)),
     uq as (
         select * from q1
         union all
         select * from q2
         union all
         select * from q3
     )
select environment_name, tenant_name, job_name,parameter_name,value, source_code,   starter_source_code, priority,
       row_number() over ( partition by environment_name, tenant_name, job_name order by priority) row_number
       from uq;



SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE 
VIEW v_iws_job_chain_tenant AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


COMMENT ON TABLE "JOB_REPO"."V_IWS_JOB_CHAIN_TENANT" IS 'Used for documentation purposes to store all job names ';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_tenant_neu AS   WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tpa2.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    upper(tpa.value)  "WRAPPER_SCRIPT"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
Left join tenant_parameter tpa ON(
    tpa.tenant_id =  et.tenant_id and tpa.PARAMETER = 'WRAPPER_SCRIPT'
        )
Left join tenant_parameter tpa2 ON(
    tpa2.tenant_id =  et.tenant_id and tpa2.PARAMETER = 'JC_TASK_PATH'
        )
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || lower(nvl(wrapper_script,job_name)) || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ) ) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final_test AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ),
    XMLELEMENT(NAME "model:Resource",
    XMLATTRIBUTES(MIN(JC_RES_DB2_DEFAULT_VALUE) AS "name",
    '2' AS "units",
    MIN(JC_JD_WORKSTATION) AS "workstation"))) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;








SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE     VIEW JOB_REPO.v_generated_scripts_FINAL AS WITH jb (environment_name,
                                                        job_name,
                                                        parameter_name,
                                                        value,
                                                        source_code,
                                                        starter_source_code,
                                                        ROW_NUMBER) AS (
    SELECT environment_name,
           LOWER(job_name) job_name,
           parameter_name,
           value,
           REPLACE(source_code, '@@@' || parameter_name || '@@@', value),
        REPLACE(starter_source_code, '@@@' || parameter_name || '@@@', value),
           ROW_NUMBER
    FROM JOB_REPO.V_job_parameter_env_ten
    WHERE ROW_NUMBER = 1
    UNION ALL
    SELECT jb.environment_name,
           LOWER(jb.job_name) job_name,
           p.parameter_name,
           p.value,
           REPLACE(jb.source_code, '@@@' || p.parameter_name || '@@@', p.value),
            REPLACE(jb.starter_source_code, '@@@' || p.parameter_name || '@@@', p.value),
           jb.row_number + 1
    FROM jb,
         JOB_REPO.V_job_parameter_env_ten p
    WHERE p.job_name = jb.job_name
      AND p.row_number = jb.row_number + 1)
                                               SELECT DISTINCT environment_name,
                                                               job_name,
                                                               source_code,
                                                               starter_source_code
                                               FROM jb
                                               WHERE ROW_NUMBER IN (
                                                   SELECT MAX(ROW_NUMBER)
                                                   FROM jb i
                                                   WHERE i.job_name = jb.job_name
                                                     AND i.environment_name = jb.environment_name);








SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_tenant_neu AS   WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tpa2.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    upper(tpa.value)  "WRAPPER_SCRIPT"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
Left join tenant_parameter tpa ON(
    tpa.tenant_id =  et.tenant_id and tpa.PARAMETER = 'WRAPPER_SCRIPT'
        )
Left join tenant_parameter tpa2 ON(
    tpa2.tenant_id =  et.tenant_id and tpa2.PARAMETER = 'JC_TASK_PATH'
        )
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || lower(nvl(wrapper_script,job_name)) || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;



SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW JOB_REPO.V_TEC_JOBS (job_id,
job_name,
job_group_id,
parameter_group_id,
parameter_group_value_id,
script_id,
starter_script_id,
parameter_name,
value,
string_flag,
DATABASE,
TABLE
    ,
    filter_condition,
    selection_field,
    reference_field_target,
    field_transformation,
    source_code,
    starter_source_code
    ) AS SELECT
    j.job_id,
    j.JOB_NAME,
    j.job_group_id,
    j.script_id,
                            j.starter_script_id,
    j.parameter_group_id,
    NULL parameter_group_value_id,
    NULL parameter_name,
    NULL value,
    NULL string_flag,
    j.DATABASE,
    j.TABLE,
    NULL filter_condition,
    NULL selection_field,
    NULL reference_field_target,
    NULL field_transformation,
    s.source_code,
    ss.source_code starter_source_code
FROM
    JOB_REPO.job j
LEFT JOIN JOB_REPO.script s ON
    (s.script_id = j.script_id)
LEFT JOIN JOB_REPO.starter_script ss ON
    (ss.starter_script_id = j.starter_script_id)
WHERE
    parameter_group_id IS NULL
UNION ALL SELECT
    j.job_id,
    j.JOB_NAME || '_' || pgv.VALUE,
    j.job_group_id,
                  j.script_id,
                 j.starter_script_id,
    j.parameter_group_id,
    pgv.PARAMETER_GROUP_VALUE_ID,

    pg.parameter_name,
    pgv.VALUE,
    pgv.STRING_FLAG,
    j.DATABASE,
    pgv.VALUE,
    pgv.FILTER_CONDITION,
    pgv.selection_field,
    pgv.reference_field_target,
    pgv.FIELD_TRANSFORMATION,
    s.source_code,
    ss.source_code starter_source_code
FROM
    JOB_REPO.JOB j
JOIN JOB_REPO.PARAMETER_GROUP pg ON
    (j.PARAMETER_GROUP_ID = pg.PARAMETER_GROUP_ID)
JOIN JOB_REPO.PARAMETER_GROUP_VALUE pgv ON
    (pg.PARAMETER_GROUP_ID = pgv.PARAMETER_GROUP_ID)
LEFT JOIN JOB_REPO.script s ON
    (s.script_id = j.script_id)
LEFT JOIN JOB_REPO.starter_script ss ON
    (ss.starter_script_id = j.starter_script_id);


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view JOB_REPO.V_JOBS AS
SELECT JOB_NAME
FROM JOB_REPO.V_TEC_JOBS;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view JOB_REPO.V_JOB_DEPENDENCIES (PARENT_JOB_NAME, child_JOB_NAME)
AS
    SELECT src.JOB_NAME PARENT_JOB_NAME, tgt.job_name CHILD_JOB_NAME
    FROM JOB_REPO.V_TEC_JOBS src
             join JOB_REPO.JOB_DEPENDENCY jd on (jd.PARENT_JOB_ID = src.JOB_ID)
             join JOB_REPO.V_TEC_JOBS tgt on (jd.CHILD_JOB_ID = tgt.JOB_ID and tgt.VALUE = src.value)
    where jd.PARENT_PARAMETER_GROUP_VALUE_ID is null
      and jd.CHILD_PARAMETER_GROUP_VALUE_ID is null
    union
    SELECT src.JOB_NAME PARENT_JOB_NAME, tgt.job_name
    FROM JOB_REPO.V_TEC_JOBS src
             join JOB_REPO.JOB_DEPENDENCY jd on (jd.PARENT_JOB_ID = src.JOB_ID)
             join JOB_REPO.V_TEC_JOBS tgt on (jd.CHILD_JOB_ID = tgt.JOB_ID)
    where tgt.VALUE is null
      and src.VALUE is not null
      and jd.PARENT_PARAMETER_GROUP_VALUE_ID is null
      and jd.CHILD_PARAMETER_GROUP_VALUE_ID is null
    union
    SELECT src.JOB_NAME PARENT_JOB_NAME, tgt.job_name
    FROM JOB_REPO.V_TEC_JOBS src
             join JOB_REPO.JOB_DEPENDENCY jd on (jd.PARENT_JOB_ID = src.JOB_ID)
             join  JOB_REPO.V_TEC_JOBS tgt on (jd.CHILD_JOB_ID = tgt.JOB_ID)
    where tgt.VALUE is not null
      and src.VALUE is null
      and jd.PARENT_PARAMETER_GROUP_VALUE_ID is null
      and jd.CHILD_PARAMETER_GROUP_VALUE_ID is null
    union
    SELECT src.JOB_NAME PARENT_JOB_NAME, tgt.job_name
    FROM JOB_REPO.V_TEC_JOBS src
             join JOB_REPO.JOB_DEPENDENCY jd on (jd.PARENT_JOB_ID = src.JOB_ID)
             join JOB_REPO.V_TEC_JOBS tgt on (jd.CHILD_JOB_ID = tgt.JOB_ID)
    where tgt.VALUE is null
      and src.VALUE is null
      and jd.PARENT_PARAMETER_GROUP_VALUE_ID is null
      and jd.CHILD_PARAMETER_GROUP_VALUE_ID is null
    UNION
    SELECT src.JOB_NAME PARENT_JOB_NAME, tgt.JOB_NAME
    FROM JOB_REPO.V_TEC_JOBS srC
             join JOB_REPO.JOB_DEPENDENCY jd on (jd.PARENT_JOB_ID = src.JOB_ID)
             join JOB_REPO.V_TEC_JOBS tgt on (jd.CHILD_JOB_ID = tgt.JOB_ID)
    where tgt.VALUE is null
      and src.VALUE is not null
      and jd.parent_PARAMETER_GROUP_VALUE_ID is not null
      and jd.CHILD_PARAMETER_GROUP_VALUE_ID is null
      and jd.parent_PARAMETER_GROUP_VALUE_ID = src.PARAMETER_GROUP_VALUE_ID;


COMMENT ON TABLE "JOB_REPO"."V_JOB_DEPENDENCIES" IS ' Adds the table name from the job_dependency table. Child job starts after finishing parent job. Ex: Core Integration(CI) starts after Rawvault Integration(RI) ';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW JOB_REPO.v_JOB_PARAMETER (JOB_name,
PARAMETER_NAME,
VALUE,
STRING_FLAG,
source_code,
starter_source_code,
ROW_NUMBER,
filter_column,
job_control_sort,
priority) AS  WITH un AS (
SELECT
    j.JOB_name,
    p.PARAMETER_NAME,
    jp.VALUE,
    jp.string_flag,
    j.source_code,
    j.starter_source_code,
    p.JOB_CONTROL_SORT,
    j.FILTER_CONDITION,
    1 priority
FROM
    JOB_REPO.V_tec_jobs j
JOIN JOB_REPO.job_parameter jp ON
    (jp.job_id = j.job_id)
JOIN JOB_REPO.parameter p ON
    (p.parameter_id = jp.PARAMETER_ID)
    -- where j.DATABASE is null
UNION
SELECT
    j.job_name,
    b.PARAMETER_NAME parameter_name,
    CAST(GP.VALUE AS VARCHAR(1000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    3 priority
FROM
    JOB_REPO.v_tec_jobs j
JOIN JOB_REPO.JOB_GROUP_PARAMETER gp ON
    (j.JOB_GROUP_ID = gp.JOB_GROUP_ID)
JOIN JOB_REPO.parameter B ON
    (B.PARAMETER_ID = gp.PARAMETER_ID)
UNION
-- Job Chain Parameter FFF

    SELECT j.JOB_name,
    p.PARAMETER_NAME,
    jp.VALUE,
    TRUE string_flag,
    j.source_code,
    j.starter_source_code,
    p.JOB_CONTROL_SORT,
    NULL filter_column,
    3 priority
FROM
    JOB_REPO.V_tec_jobs j
JOIN JOB_REPO.job_chain_parameter jp ON
    (jp.job_chain_id = 'PROD'
    AND jp.ENVIRONMENT_ID = 'PROD')
JOIN JOB_REPO.parameter p ON
    (p.parameter_id = jp.PARAMETER_ID)
    --where j.DATABASE is null
UNION
-- Table names from parameter group

    SELECT j.job_name,
    'FILTER_CONDITION' parameter_name,
    nvl(j.FILTER_CONDITION,
    '') ,
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    5 priority
FROM
    JOB_REPO.v_tec_jobs j
WHERE
    PARAMETER_NAME IS NOT NULL
    AND FILTER_CONDITION IS NOT NULL
    AND FILTER_CONDITION <> ''
    --  and j.DATABASE is NULL
UNION
-- Table names from parameter group

    SELECT j.job_name,
    'FIELD_TRANSFORMATION' parameter_name,
    nvl(j.FIELD_TRANSFORMATION,
    '') ,
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL FIELD_TRANSFORMATION,
    5 priority
FROM
    JOB_REPO.v_tec_jobs j
WHERE
    PARAMETER_NAME IS NOT NULL
    AND j.FIELD_TRANSFORMATION IS NOT NULL
    --  and j.DATABASE is NULL
UNION
-- Table names from parameter group

    SELECT j.job_name,
    j.parameter_name,
    j.VALUE,
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    5 priority
FROM
    JOB_REPO.v_tec_jobs j
WHERE
    PARAMETER_NAME IS NOT NULL
UNION
-- Table names from parameter group

    SELECT j.job_name,
    'SELECTION_FIELD' parameter_name,
    nvl(j.FILTER_CONDITION,
    '') ,
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    6 priority
FROM
    JOB_REPO.v_tec_jobs j
WHERE
    PARAMETER_NAME IS NOT NULL
    --  and j.DATABASE is NULL
UNION
-- Table names from parameter group

    SELECT j.job_name,
    'REFERENCE_FIELD_TARGET' parameter_name,
    nvl(j.FILTER_CONDITION,
    '') ,
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    7 priority
FROM
    JOB_REPO.v_tec_jobs j
WHERE
    PARAMETER_NAME IS NOT NULL
    --  and j.DATABASE is NULL
UNION
-- keys (ID and SID)

    SELECT j.job_name,
    b.PARAMETER parameter_name,
    CAST(b.KEY AS VARCHAR(1000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    9 priority
    --from JOB_REPO.v_tec_jobs j

    FROM JOB_REPO.v_tec_jobs j
JOIN TABLE_REPO.V_TABLE_KEY_PARAMETER b ON
    --   (LOWER(j.database) = LOWER(b.DATABASE) AND
(LOWER(j.table) = LOWER(b.table))
WHERE
    -- j.DATABASE IS NOT NULL
 j.TABLE IS NOT NULL
UNION
-- foreign keys

    SELECT j.job_name,
    b.PARAMETER parameter_name,
    CAST(b.STR_JSON AS VARCHAR(30000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    10 priority
FROM
    JOB_REPO.v_tec_jobs j
JOIN TABLE_REPO.V_TABLE_FOREIGN_KEY_PARAMETER b ON
    (LOWER(j.database) = LOWER(b.DATABASE_CORE)
    AND LOWER(j.table) = LOWER(b.TABLE_NAME_DWH))
WHERE
    j.DATABASE IS NOT NULL
    -- and j.TABLE is not null
UNION
-- foreign keys

    SELECT j.job_name,
    b.PARAMETER_NAME parameter_name,
    CAST(b.STR_JSON AS VARCHAR(30000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    11 priority
FROM
    JOB_REPO.v_tec_jobs j
JOIN TABLE_REPO.V_TABLE_DB2_EXTRACTION b ON
    (LOWER(j.database) = LOWER(b.DATABASE_CORE)
    AND LOWER(j.table) = LOWER(b.TABLE_NAME))
WHERE
    j.DATABASE IS NOT NULL
UNION
--join tables

    SELECT j.job_name,
    b.PARAMETER parameter_name,
    CAST(b.STRING_JSON AS VARCHAR(30000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    11 priority
FROM
    JOB_REPO.V_TEC_JOBS j
JOIN TABLE_REPO.V_TABLE_JOIN b ON
    (LOWER(j.database) = LOWER(b.DATABASE_NAME)
    AND LOWER(j.table) = LOWER(b.TABLE_NAME_DWH))
WHERE
    j.DATABASE IS NOT NULL
UNION
--join jobname itselves

    SELECT j.job_name,
    'JOB_NAME',
    lower(j.job_name),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    15 priority
FROM
    JOB_REPO.V_TEC_JOBS j
UNION
--join tables

    SELECT j.job_name,
    b.PARAMETER parameter_name,
    CAST(b.STRING_JSON AS VARCHAR(30000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    12 priority
FROM
    JOB_REPO.V_TEC_JOBS j
JOIN TABLE_REPO.V_TABLE_FILTER_CONDITION b ON
    (LOWER(j.database) = LOWER(b.DATABASE_NAME)
    AND LOWER(j.table) = LOWER(b.TABLE_NAME_DWH))
WHERE
    j.DATABASE IS NOT NULL ) SELECT
    JOB_name,
    PARAMETER_NAME,
    VALUE,
    STRING_FLAG,
    source_code,
    starter_source_code,
    ROW_NUMBER() OVER (PARTITION BY job_name
ORDER BY
    parameter_name) ROW_NUMBER,
    job_control_sort,
    NULL filter_column,
    priority
FROM
    un;




SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_job_dependencies_final AS
SELECT distinct v.Tenant_name,
       tenant_name || '_' || a.CHILD_JOB_NAME CHILD_JOB_NAME,
       tenant_name || '_' || a.PARENT_JOB_NAME PARENT_JOB_NAME
FROM V_JOB_DEPENDENCIES a
         CROSS JOIN V_ENVIRONMENT_TENANT v;


COMMENT ON TABLE "JOB_REPO"."V_JOB_DEPENDENCIES_FINAL" IS ' Adds the Tenant name(itergo) to V_JOB_DEPENDECIES. Child job starts after finishing parent job. Ex: Core Integration(CI) starts after Rawvault Integration(RI) ';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view v_iws_docu_job_par as
with dec as (
    select ENVIRONMENT_NAME, e.TENANT_NAME, upper( JOB_NAME) JOB_NAME, parameter_name, value
    from V_JOB_PARAMETER
    cross join V_ENVIRONMENT_TENANT e
    where parameter_name LIKE 'JC%'
    and e.ENVIRONMENT_ID like 'PROD'
),
     exec as (
         select a.job_name, 'model:task' as element ,'exec' as attribut, a.value || TENANT_NAME || '/' || lower( a.job_name) || '.sh' value
         from dec a
         where a.parameter_name = 'JC_TASK_PATH'
     ),
     recovery as (
         select a.job_name, 'model:recovery' as element ,'action' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_REC_ACTION'
     ),
     rerun as (
         select a.job_name, 'model:recovery' as element ,'abendPrompt' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_REC_ABEND_PROMPT'
     ),
     logon as (
         select a.job_name, 'model:JobDefinition' as element ,'logon' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_LOGON'
     ),
     folder as (
         select a.job_name, 'model:JobDefinition' as element, 'folder' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     workstation as (
         select a.job_name, 'model:JobDefinition' as element, 'workstation' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_WORKSTATION'
     ),
     name as (
         select a.job_name, 'model:JobDefinition' as element, 'name' as attribut, '$' || a.value || '_' ||  a.job_name || '$' value
         from dec a
         where a.parameter_name = 'JC_JD_JOB_PREFIX'
     ),
     js_name as (
         select a.job_name, 'model:JobStream' as element, 'name' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JS_NAME'
     ),
     js_priority as (
         select a.job_name, 'model:JobStream' as element, 'priority' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JOB_PRIORITY'
     ),
     js_folder as (
         select a.job_name, 'model:JobStream' as element, 'folder' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_confirmed as (
         select a.job_name, 'model:job' as element, 'confirmed' as attribut,  'false' value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_iscritical as (
         select a.job_name, 'model:job' as element, 'iscritical' as attribut,  'false'  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_key as (
         select a.job_name, 'model:job' as element, 'iskey' as attribut,   'false'  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
    jd_name as (
         select a.job_name, 'model:job' as element, 'name' as attribut, '$' || a.value || '_' ||  a.job_name || '$' value
         from dec a
         where a.parameter_name = 'JC_JD_JOB_PREFIX'
     ),
     jd_priority as (
         select a.job_name, 'model:job' as element, 'priority' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JOB_PRIORITY'
     ),
     jd_definition as (
         select a.job_name, 'model:job' as element, 'definition' as attribut, a.value || '#' || b.value  || '#' || n.value value
         from dec a
         join dec b on (a.JOB_NAME = b.JOB_NAME and b.parameter_name = 'JC_JD_FOLDER')
         join name n on (a.JOB_NAME = n.JOB_NAME)
         where a.parameter_name = 'JC_JD_WORKSTATION'

     ),
     jd_pred as(
          select distinct a.job_name, 'model:predecessor' as element, 'target' as attribut, b.PARENT_JOB_NAME value
         from dec a
         join V_JOB_DEPENDENCIES b on (lower(b.CHILD_JOB_NAME) = lower(a.JOB_NAME))

     ),
     jd_succ as(
        select distinct a.job_name, 'model:status' as element, 'name' as attribut,   'SUCC'  value
         from  jd_pred a
     ),
     all_par as (
select *
from exec
union all
select *
from logon
union all
select *
from folder
union all
select *
from workstation
    union all
select *
from recovery
union all
select *
from rerun
union all
select *
from name
union all
select *
from js_name
union all
select *
from js_priority
union all
select *
from js_folder
union all
select *
from jd_confirmed
union all
select *
from jd_iscritical
union all
select *
from jd_key
union all
select *
from jd_name
union all
select *
from jd_priority
union all
select *
from jd_definition
union all
select *
from  jd_pred
    union all
select *
from
          jd_succ
         )

select * from all_par;



SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE 
VIEW v_iws_job_chain_tenant AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


COMMENT ON TABLE "JOB_REPO"."V_IWS_JOB_CHAIN_TENANT" IS 'Used for documentation purposes to store all job names ';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view job_repo.v_iws_docu_job_parameter as
    with dec as (
        select upper(JOB_NAME) JOB_NAME, parameter_name, value
        from V_JOB_PARAMETER
        where parameter_name LIKE 'JC%'
    ),
         RES AS (
             select JOB_NAME,
                    case
                        when parameter_name like 'JC_JD_LOGIN' THEN 'LOGIN'
                        WHEN parameter_name like 'JC_JD_LOGON' THEN 'LOGON'
                        WHEN parameter_name like 'JC_JD_JOB_PREFIX' THEN 'PREFIX'
                        WHEN parameter_name like 'JC_JD_FOLDER' THEN 'FOLDER'
                        WHEN parameter_name like 'JC_JD_WORKSTATION' THEN 'WORKSTATION'
                        WHEN parameter_name like 'JC_JOB_PRIORITY' THEN 'JOB_PRIORITY'
                        WHEN parameter_name like 'JC_REC_ABEND_PROMPT' THEN 'ABEND_PROMPT'
                        WHEN parameter_name like 'JC_JS_NAME' THEN 'JOB_STREAM_NAME'
                        WHEN parameter_name like 'JC_TASK_PATH' THEN 'TASK_PATH'
                        WHEN parameter_name like 'JC_REC_ACTION' THEN 'RECOVERY ACTION'
                        WHEN parameter_name like 'JC_NAME' THEN 'JOB_CHAIN_NAME'
                        ELSE PARAMETER_NAME END PARAMETER,
                    VALUE                       PARAMETER_VALUE
             from dec)
    SELECT JOB_NAME, parameter, parameter_value
    from res;


COMMENT ON TABLE "JOB_REPO"."V_IWS_DOCU_JOB_PARAMETER" IS 'Used for providing docu for batch team. This view is still in construction. Need other parameters update';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view v_iws_docu_job_par as
with dec as (
    select ENVIRONMENT_NAME, e.TENANT_NAME, upper( JOB_NAME) JOB_NAME, parameter_name, value
    from V_JOB_PARAMETER
    cross join V_ENVIRONMENT_TENANT e
    where parameter_name LIKE 'JC%'
    and e.ENVIRONMENT_ID like 'PROD'
),
     exec as (
         select a.job_name, 'model:task' as element ,'exec' as attribut, a.value || TENANT_NAME || '/' || lower( a.job_name) || '.sh' value
         from dec a
         where a.parameter_name = 'JC_TASK_PATH'
     ),
     recovery as (
         select a.job_name, 'model:recovery' as element ,'action' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_REC_ACTION'
     ),
     rerun as (
         select a.job_name, 'model:recovery' as element ,'abendPrompt' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_REC_ABEND_PROMPT'
     ),
     logon as (
         select a.job_name, 'model:JobDefinition' as element ,'logon' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_LOGON'
     ),
     folder as (
         select a.job_name, 'model:JobDefinition' as element, 'folder' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     workstation as (
         select a.job_name, 'model:JobDefinition' as element, 'workstation' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_WORKSTATION'
     ),
     name as (
         select a.job_name, 'model:JobDefinition' as element, 'name' as attribut, '$' || a.value || '_' ||  a.job_name || '$' value
         from dec a
         where a.parameter_name = 'JC_JD_JOB_PREFIX'
     ),
     js_name as (
         select a.job_name, 'model:JobStream' as element, 'name' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JS_NAME'
     ),
     js_priority as (
         select a.job_name, 'model:JobStream' as element, 'priority' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JOB_PRIORITY'
     ),
     js_folder as (
         select a.job_name, 'model:JobStream' as element, 'folder' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_confirmed as (
         select a.job_name, 'model:job' as element, 'confirmed' as attribut,  'false' value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_iscritical as (
         select a.job_name, 'model:job' as element, 'iscritical' as attribut,  'false'  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_key as (
         select a.job_name, 'model:job' as element, 'iskey' as attribut,   'false'  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
    jd_name as (
         select a.job_name, 'model:job' as element, 'name' as attribut, '$' || a.value || '_' ||  a.job_name || '$' value
         from dec a
         where a.parameter_name = 'JC_JD_JOB_PREFIX'
     ),
     jd_priority as (
         select a.job_name, 'model:job' as element, 'priority' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JOB_PRIORITY'
     ),
     jd_definition as (
         select a.job_name, 'model:job' as element, 'definition' as attribut, a.value || '#' || b.value  || '#' || n.value value
         from dec a
         join dec b on (a.JOB_NAME = b.JOB_NAME and b.parameter_name = 'JC_JD_FOLDER')
         join name n on (a.JOB_NAME = n.JOB_NAME)
         where a.parameter_name = 'JC_JD_WORKSTATION'

     ),
     jd_pred as(
          select distinct a.job_name, 'model:predecessor' as element, 'target' as attribut, b.PARENT_JOB_NAME value
         from dec a
         join V_JOB_DEPENDENCIES b on (lower(b.CHILD_JOB_NAME) = lower(a.JOB_NAME))

     ),
     jd_succ as(
        select distinct a.job_name, 'model:status' as element, 'name' as attribut,   'SUCC'  value
         from  jd_pred a
     ),
     all_par as (
select *
from exec
union all
select *
from logon
union all
select *
from folder
union all
select *
from workstation
    union all
select *
from recovery
union all
select *
from rerun
union all
select *
from name
union all
select *
from js_name
union all
select *
from js_priority
union all
select *
from js_folder
union all
select *
from jd_confirmed
union all
select *
from jd_iscritical
union all
select *
from jd_key
union all
select *
from jd_name
union all
select *
from jd_priority
union all
select *
from jd_definition
union all
select *
from  jd_pred
    union all
select *
from
          jd_succ
         )

select * from all_par;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_tenant_neu AS   WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tpa2.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    upper(tpa.value)  "WRAPPER_SCRIPT"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
Left join tenant_parameter tpa ON(
    tpa.tenant_id =  et.tenant_id and tpa.PARAMETER = 'WRAPPER_SCRIPT'
        )
Left join tenant_parameter tpa2 ON(
    tpa2.tenant_id =  et.tenant_id and tpa2.PARAMETER = 'JC_TASK_PATH'
        )
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || lower(nvl(wrapper_script,job_name)) || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ) ) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final_test AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ),
    XMLELEMENT(NAME "model:Resource",
    XMLATTRIBUTES(MIN(JC_RES_DB2_DEFAULT_VALUE) AS "name",
    '2' AS "units",
    MIN(JC_JD_WORKSTATION) AS "workstation"))) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE     VIEW JOB_REPO.v_generated_scripts (job_name,
                                       source_code, starter_source_code) AS
WITH jb (job_name,
         parameter_name,
         value,
         source_code,
         starter_source_code,
         ROW_NUMBER) AS (
    SELECT job_name,
           parameter_name,
           value,
           REPLACE(source_code, '@@@' || parameter_name || '@@@', value),
           REPLACE(starter_source_code, '@@@' || parameter_name || '@@@', value),
           ROW_NUMBER
    FROM JOB_REPO.V_job_parameter
    WHERE ROW_NUMBER = 1
    UNION ALL
    SELECT jb.job_name,
           p.parameter_name,
           p.value,
           REPLACE(jb.source_code, '@@@' || p.parameter_name || '@@@', p.value),
           REPLACE(jb.starter_source_code, '@@@' || p.parameter_name || '@@@', p.value),
           jb.row_number + 1
    FROM jb,
         JOB_REPO.V_job_parameter p
    WHERE p.job_name = jb.job_name
      AND p.row_number = jb.row_number + 1)
SELECT jb.job_name,
       jb.source_code,
       jb.starter_source_code
FROM jb
WHERE ROW_NUMBER IN (
    SELECT MAX(ROW_NUMBER)
    FROM jb i
    WHERE i.job_name = jb.job_name);



SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE 
VIEW v_iws_job_chain_tenant AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


COMMENT ON TABLE "JOB_REPO"."V_IWS_JOB_CHAIN_TENANT" IS 'Used for documentation purposes to store all job names ';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_tenant_neu AS   WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tpa2.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    upper(tpa.value)  "WRAPPER_SCRIPT"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
Left join tenant_parameter tpa ON(
    tpa.tenant_id =  et.tenant_id and tpa.PARAMETER = 'WRAPPER_SCRIPT'
        )
Left join tenant_parameter tpa2 ON(
    tpa2.tenant_id =  et.tenant_id and tpa2.PARAMETER = 'JC_TASK_PATH'
        )
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || lower(nvl(wrapper_script,job_name)) || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ) ) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final_test AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ),
    XMLELEMENT(NAME "model:Resource",
    XMLATTRIBUTES(MIN(JC_RES_DB2_DEFAULT_VALUE) AS "name",
    '2' AS "units",
    MIN(JC_JD_WORKSTATION) AS "workstation"))) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;










SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view V_JOB_PARAMETER_ENV_TEN as
with q1 as (
select e.environment_name,
       e.tenant_name,
       e.tenant_name ||'_' || v.job_name job_name,
       e.PARAMTER_ENVIRONMENT_ID parameter_name,
       e.environment_name        value,
       1                         priority,
       v.SOURCE_CODE,
       v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e),
     q2 as(
select e.environment_name,
       e.tenant_name,
        e.tenant_name ||'_' || v.job_name job_name,
       e.PARAMETER_TENANT_ID parameter_name,
       e.TENANT_NAME         value,
       2                     priority,
       v.SOURCE_CODE,
         v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e),
     q3 as(
select e.environment_name,
       e.tenant_name,
        e.tenant_name ||'_' || v.job_name job_name,
       p.PARAMETER parameter_name,
       p.DATABASE,
       3 priority,
       v.SOURCE_CODE,
         v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e
        join JOB_REPO.database_parameter p on (p.ENVIRONMENT_ID = e.ENVIRONMENT_id and e.TENANT_ID = p.TENANT_ID)),
     uq as (
         select * from q1
         union all
         select * from q2
         union all
         select * from q3
     )
select environment_name, tenant_name, job_name,parameter_name,value, source_code,   starter_source_code, priority,
       row_number() over ( partition by environment_name, tenant_name, job_name order by priority) row_number
       from uq;







SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE     VIEW JOB_REPO.v_generated_scripts_FINAL AS WITH jb (environment_name,
                                                        job_name,
                                                        parameter_name,
                                                        value,
                                                        source_code,
                                                        starter_source_code,
                                                        ROW_NUMBER) AS (
    SELECT environment_name,
           LOWER(job_name) job_name,
           parameter_name,
           value,
           REPLACE(source_code, '@@@' || parameter_name || '@@@', value),
        REPLACE(starter_source_code, '@@@' || parameter_name || '@@@', value),
           ROW_NUMBER
    FROM JOB_REPO.V_job_parameter_env_ten
    WHERE ROW_NUMBER = 1
    UNION ALL
    SELECT jb.environment_name,
           LOWER(jb.job_name) job_name,
           p.parameter_name,
           p.value,
           REPLACE(jb.source_code, '@@@' || p.parameter_name || '@@@', p.value),
            REPLACE(jb.starter_source_code, '@@@' || p.parameter_name || '@@@', p.value),
           jb.row_number + 1
    FROM jb,
         JOB_REPO.V_job_parameter_env_ten p
    WHERE p.job_name = jb.job_name
      AND p.row_number = jb.row_number + 1)
                                               SELECT DISTINCT environment_name,
                                                               job_name,
                                                               source_code,
                                                               starter_source_code
                                               FROM jb
                                               WHERE ROW_NUMBER IN (
                                                   SELECT MAX(ROW_NUMBER)
                                                   FROM jb i
                                                   WHERE i.job_name = jb.job_name
                                                     AND i.environment_name = jb.environment_name);




SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW JOB_REPO.v_JOB_PARAMETER (JOB_name,
PARAMETER_NAME,
VALUE,
STRING_FLAG,
source_code,
starter_source_code,
ROW_NUMBER,
filter_column,
job_control_sort,
priority) AS  WITH un AS (
SELECT
    j.JOB_name,
    p.PARAMETER_NAME,
    jp.VALUE,
    jp.string_flag,
    j.source_code,
    j.starter_source_code,
    p.JOB_CONTROL_SORT,
    j.FILTER_CONDITION,
    1 priority
FROM
    JOB_REPO.V_tec_jobs j
JOIN JOB_REPO.job_parameter jp ON
    (jp.job_id = j.job_id)
JOIN JOB_REPO.parameter p ON
    (p.parameter_id = jp.PARAMETER_ID)
    -- where j.DATABASE is null
UNION
SELECT
    j.job_name,
    b.PARAMETER_NAME parameter_name,
    CAST(GP.VALUE AS VARCHAR(1000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    3 priority
FROM
    JOB_REPO.v_tec_jobs j
JOIN JOB_REPO.JOB_GROUP_PARAMETER gp ON
    (j.JOB_GROUP_ID = gp.JOB_GROUP_ID)
JOIN JOB_REPO.parameter B ON
    (B.PARAMETER_ID = gp.PARAMETER_ID)
UNION
-- Job Chain Parameter FFF

    SELECT j.JOB_name,
    p.PARAMETER_NAME,
    jp.VALUE,
    TRUE string_flag,
    j.source_code,
    j.starter_source_code,
    p.JOB_CONTROL_SORT,
    NULL filter_column,
    3 priority
FROM
    JOB_REPO.V_tec_jobs j
JOIN JOB_REPO.job_chain_parameter jp ON
    (jp.job_chain_id = 'PROD'
    AND jp.ENVIRONMENT_ID = 'PROD')
JOIN JOB_REPO.parameter p ON
    (p.parameter_id = jp.PARAMETER_ID)
    --where j.DATABASE is null
UNION
-- Table names from parameter group

    SELECT j.job_name,
    'FILTER_CONDITION' parameter_name,
    nvl(j.FILTER_CONDITION,
    '') ,
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    5 priority
FROM
    JOB_REPO.v_tec_jobs j
WHERE
    PARAMETER_NAME IS NOT NULL
    AND FILTER_CONDITION IS NOT NULL
    AND FILTER_CONDITION <> ''
    --  and j.DATABASE is NULL
UNION
-- Table names from parameter group

    SELECT j.job_name,
    'FIELD_TRANSFORMATION' parameter_name,
    nvl(j.FIELD_TRANSFORMATION,
    '') ,
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL FIELD_TRANSFORMATION,
    5 priority
FROM
    JOB_REPO.v_tec_jobs j
WHERE
    PARAMETER_NAME IS NOT NULL
    AND j.FIELD_TRANSFORMATION IS NOT NULL
    --  and j.DATABASE is NULL
UNION
-- Table names from parameter group

    SELECT j.job_name,
    j.parameter_name,
    j.VALUE,
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    5 priority
FROM
    JOB_REPO.v_tec_jobs j
WHERE
    PARAMETER_NAME IS NOT NULL
UNION
-- Table names from parameter group

    SELECT j.job_name,
    'SELECTION_FIELD' parameter_name,
    nvl(j.FILTER_CONDITION,
    '') ,
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    6 priority
FROM
    JOB_REPO.v_tec_jobs j
WHERE
    PARAMETER_NAME IS NOT NULL
    --  and j.DATABASE is NULL
UNION
-- Table names from parameter group

    SELECT j.job_name,
    'REFERENCE_FIELD_TARGET' parameter_name,
    nvl(j.FILTER_CONDITION,
    '') ,
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    7 priority
FROM
    JOB_REPO.v_tec_jobs j
WHERE
    PARAMETER_NAME IS NOT NULL
    --  and j.DATABASE is NULL
UNION
-- keys (ID and SID)

    SELECT j.job_name,
    b.PARAMETER parameter_name,
    CAST(b.KEY AS VARCHAR(1000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    9 priority
    --from JOB_REPO.v_tec_jobs j

    FROM JOB_REPO.v_tec_jobs j
JOIN TABLE_REPO.V_TABLE_KEY_PARAMETER b ON
    --   (LOWER(j.database) = LOWER(b.DATABASE) AND
(LOWER(j.table) = LOWER(b.table))
WHERE
    -- j.DATABASE IS NOT NULL
 j.TABLE IS NOT NULL
UNION
-- foreign keys

    SELECT j.job_name,
    b.PARAMETER parameter_name,
    CAST(b.STR_JSON AS VARCHAR(30000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    10 priority
FROM
    JOB_REPO.v_tec_jobs j
JOIN TABLE_REPO.V_TABLE_FOREIGN_KEY_PARAMETER b ON
    (LOWER(j.database) = LOWER(b.DATABASE_CORE)
    AND LOWER(j.table) = LOWER(b.TABLE_NAME_DWH))
WHERE
    j.DATABASE IS NOT NULL
    -- and j.TABLE is not null
UNION
-- foreign keys

    SELECT j.job_name,
    b.PARAMETER_NAME parameter_name,
    CAST(b.STR_JSON AS VARCHAR(30000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    11 priority
FROM
    JOB_REPO.v_tec_jobs j
JOIN TABLE_REPO.V_TABLE_DB2_EXTRACTION b ON
    (LOWER(j.database) = LOWER(b.DATABASE_CORE)
    AND LOWER(j.table) = LOWER(b.TABLE_NAME))
WHERE
    j.DATABASE IS NOT NULL
UNION
--join tables

    SELECT j.job_name,
    b.PARAMETER parameter_name,
    CAST(b.STRING_JSON AS VARCHAR(30000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    11 priority
FROM
    JOB_REPO.V_TEC_JOBS j
JOIN TABLE_REPO.V_TABLE_JOIN b ON
    (LOWER(j.database) = LOWER(b.DATABASE_NAME)
    AND LOWER(j.table) = LOWER(b.TABLE_NAME_DWH))
WHERE
    j.DATABASE IS NOT NULL
UNION
--join jobname itselves

    SELECT j.job_name,
    'JOB_NAME',
    lower(j.job_name),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    15 priority
FROM
    JOB_REPO.V_TEC_JOBS j
UNION
--join tables

    SELECT j.job_name,
    b.PARAMETER parameter_name,
    CAST(b.STRING_JSON AS VARCHAR(30000)),
    j.STRING_FLAG,
    j.source_code,
    j.starter_source_code,
    NULL job_control_sort,
    NULL filter_column,
    12 priority
FROM
    JOB_REPO.V_TEC_JOBS j
JOIN TABLE_REPO.V_TABLE_FILTER_CONDITION b ON
    (LOWER(j.database) = LOWER(b.DATABASE_NAME)
    AND LOWER(j.table) = LOWER(b.TABLE_NAME_DWH))
WHERE
    j.DATABASE IS NOT NULL ) SELECT
    JOB_name,
    PARAMETER_NAME,
    VALUE,
    STRING_FLAG,
    source_code,
    starter_source_code,
    ROW_NUMBER() OVER (PARTITION BY job_name
ORDER BY
    parameter_name) ROW_NUMBER,
    job_control_sort,
    NULL filter_column,
    priority
FROM
    un;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE 
VIEW v_iws_job_chain_tenant AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


COMMENT ON TABLE "JOB_REPO"."V_IWS_JOB_CHAIN_TENANT" IS 'Used for documentation purposes to store all job names ';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view job_repo.v_iws_docu_job_parameter as
    with dec as (
        select upper(JOB_NAME) JOB_NAME, parameter_name, value
        from V_JOB_PARAMETER
        where parameter_name LIKE 'JC%'
    ),
         RES AS (
             select JOB_NAME,
                    case
                        when parameter_name like 'JC_JD_LOGIN' THEN 'LOGIN'
                        WHEN parameter_name like 'JC_JD_LOGON' THEN 'LOGON'
                        WHEN parameter_name like 'JC_JD_JOB_PREFIX' THEN 'PREFIX'
                        WHEN parameter_name like 'JC_JD_FOLDER' THEN 'FOLDER'
                        WHEN parameter_name like 'JC_JD_WORKSTATION' THEN 'WORKSTATION'
                        WHEN parameter_name like 'JC_JOB_PRIORITY' THEN 'JOB_PRIORITY'
                        WHEN parameter_name like 'JC_REC_ABEND_PROMPT' THEN 'ABEND_PROMPT'
                        WHEN parameter_name like 'JC_JS_NAME' THEN 'JOB_STREAM_NAME'
                        WHEN parameter_name like 'JC_TASK_PATH' THEN 'TASK_PATH'
                        WHEN parameter_name like 'JC_REC_ACTION' THEN 'RECOVERY ACTION'
                        WHEN parameter_name like 'JC_NAME' THEN 'JOB_CHAIN_NAME'
                        ELSE PARAMETER_NAME END PARAMETER,
                    VALUE                       PARAMETER_VALUE
             from dec)
    SELECT JOB_NAME, parameter, parameter_value
    from res;


COMMENT ON TABLE "JOB_REPO"."V_IWS_DOCU_JOB_PARAMETER" IS 'Used for providing docu for batch team. This view is still in construction. Need other parameters update';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view v_iws_docu_job_par as
with dec as (
    select ENVIRONMENT_NAME, e.TENANT_NAME, upper( JOB_NAME) JOB_NAME, parameter_name, value
    from V_JOB_PARAMETER
    cross join V_ENVIRONMENT_TENANT e
    where parameter_name LIKE 'JC%'
    and e.ENVIRONMENT_ID like 'PROD'
),
     exec as (
         select a.job_name, 'model:task' as element ,'exec' as attribut, a.value || TENANT_NAME || '/' || lower( a.job_name) || '.sh' value
         from dec a
         where a.parameter_name = 'JC_TASK_PATH'
     ),
     recovery as (
         select a.job_name, 'model:recovery' as element ,'action' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_REC_ACTION'
     ),
     rerun as (
         select a.job_name, 'model:recovery' as element ,'abendPrompt' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_REC_ABEND_PROMPT'
     ),
     logon as (
         select a.job_name, 'model:JobDefinition' as element ,'logon' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_LOGON'
     ),
     folder as (
         select a.job_name, 'model:JobDefinition' as element, 'folder' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     workstation as (
         select a.job_name, 'model:JobDefinition' as element, 'workstation' as attribut, a.value value
         from dec a
         where a.parameter_name = 'JC_JD_WORKSTATION'
     ),
     name as (
         select a.job_name, 'model:JobDefinition' as element, 'name' as attribut, '$' || a.value || '_' ||  a.job_name || '$' value
         from dec a
         where a.parameter_name = 'JC_JD_JOB_PREFIX'
     ),
     js_name as (
         select a.job_name, 'model:JobStream' as element, 'name' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JS_NAME'
     ),
     js_priority as (
         select a.job_name, 'model:JobStream' as element, 'priority' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JOB_PRIORITY'
     ),
     js_folder as (
         select a.job_name, 'model:JobStream' as element, 'folder' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_confirmed as (
         select a.job_name, 'model:job' as element, 'confirmed' as attribut,  'false' value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_iscritical as (
         select a.job_name, 'model:job' as element, 'iscritical' as attribut,  'false'  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
     jd_key as (
         select a.job_name, 'model:job' as element, 'iskey' as attribut,   'false'  value
         from dec a
         where a.parameter_name = 'JC_JD_FOLDER'
     ),
    jd_name as (
         select a.job_name, 'model:job' as element, 'name' as attribut, '$' || a.value || '_' ||  a.job_name || '$' value
         from dec a
         where a.parameter_name = 'JC_JD_JOB_PREFIX'
     ),
     jd_priority as (
         select a.job_name, 'model:job' as element, 'priority' as attribut,  a.value  value
         from dec a
         where a.parameter_name = 'JC_JOB_PRIORITY'
     ),
     jd_definition as (
         select a.job_name, 'model:job' as element, 'definition' as attribut, a.value || '#' || b.value  || '#' || n.value value
         from dec a
         join dec b on (a.JOB_NAME = b.JOB_NAME and b.parameter_name = 'JC_JD_FOLDER')
         join name n on (a.JOB_NAME = n.JOB_NAME)
         where a.parameter_name = 'JC_JD_WORKSTATION'

     ),
     jd_pred as(
          select distinct a.job_name, 'model:predecessor' as element, 'target' as attribut, b.PARENT_JOB_NAME value
         from dec a
         join V_JOB_DEPENDENCIES b on (lower(b.CHILD_JOB_NAME) = lower(a.JOB_NAME))

     ),
     jd_succ as(
        select distinct a.job_name, 'model:status' as element, 'name' as attribut,   'SUCC'  value
         from  jd_pred a
     ),
     all_par as (
select *
from exec
union all
select *
from logon
union all
select *
from folder
union all
select *
from workstation
    union all
select *
from recovery
union all
select *
from rerun
union all
select *
from name
union all
select *
from js_name
union all
select *
from js_priority
union all
select *
from js_folder
union all
select *
from jd_confirmed
union all
select *
from jd_iscritical
union all
select *
from jd_key
union all
select *
from jd_name
union all
select *
from jd_priority
union all
select *
from jd_definition
union all
select *
from  jd_pred
    union all
select *
from
          jd_succ
         )

select * from all_par;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_tenant_neu AS   WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tpa2.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    upper(tpa.value)  "WRAPPER_SCRIPT"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
CROSS JOIN V_ENVIRONMENT_TENANT et
Left join tenant_parameter tpa ON(
    tpa.tenant_id =  et.tenant_id and tpa.PARAMETER = 'WRAPPER_SCRIPT'
        )
Left join tenant_parameter tpa2 ON(
    tpa2.tenant_id =  et.tenant_id and tpa2.PARAMETER = 'JC_TASK_PATH'
        )
WHERE
    jn.job_name IN ('JC_START',
    'LF_FTP_BEARBNW',
    'LF_FTP_hvgruppe',
    'LF_FTP_jurlv',
    'LF_FTP_lv',
    'LF_FTP_vt',
    'LF_RAW_ZONE_BEARBNW',
    'LF_RAW_ZONE_hvgruppe',
    'LF_RAW_ZONE_jurlv',
    'LF_RAW_ZONE_lv',
    'LF_RAW_ZONE_vt',
    'LF_CORE_BEARBNW',
    'LF_CORE_hvgruppe',
    'LF_CORE_jurlv',
    'LF_CORE_lv',
    'LF_CORE_vt',
    'JC_STOP') ) SELECT TENANT_NAME,
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || lower(nvl(wrapper_script,job_name)) || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name
        AND v.PARENT_JOB_NAME IN (TENANT_NAME || '_LF_CORE_BEARBNW',
        TENANT_NAME || '_LF_CORE_hvgruppe',
        TENANT_NAME || '_LF_CORE_jurlv',
        TENANT_NAME || '_LF_CORE_lv',
        TENANT_NAME || '_LF_CORE_vt',
        TENANT_NAME || '_LF_RAW_ZONE_BEARBNW',
        TENANT_NAME || '_LF_RAW_ZONE_hvgruppe',
        TENANT_NAME || '_LF_RAW_ZONE_jurlv',
        TENANT_NAME || '_LF_RAW_ZONE_lv',
        TENANT_NAME || '_LF_RAW_ZONE_vt',
        TENANT_NAME || '_LF_FTP_BEARBNW',
        TENANT_NAME || '_LF_FTP_hvgruppe',
        TENANT_NAME || '_LF_FTP_jurlv',
        TENANT_NAME || '_LF_FTP_lv',
        TENANT_NAME || '_LF_FTP_vt',
        TENANT_NAME || '_JC_START' ) ) ) ) ) ) ) ) xml_job
        FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ) ) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW v_iws_job_chain_final_test AS WITH job_par AS (
SELECT
    DISTINCT et.TENANT_NAME,
    UPPER(et.TENANT_NAME || '_' || jn.job_name) job_name,
    jws.VALUE "JC_JD_WORKSTATION",
    jf.value "JC_JD_FOLDER",
    lg.value "JC_JD_LOGON",
    pf.value "JC_JD_JOB_PREFIX",
    tp.value "JC_TASK_PATH",
    js.value "JC_JS_NAME",
    rp.value "JC_REC_ABEND_PROMPT",
    ra.value "JC_REC_ACTION",
    rs.value "JC_RES_DB2_DEFAULT_VALUE"
FROM
    v_job_parameter jn
LEFT JOIN v_job_parameter jws ON
    (jws.JOB_NAME = jn.JOB_NAME
    AND jws.PARAMETER_NAME = 'JC_JD_WORKSTATION')
LEFT JOIN v_job_parameter jf ON
    (jf.JOB_NAME = jn.JOB_NAME
    AND jf.PARAMETER_NAME = 'JC_JD_FOLDER')
LEFT JOIN V_JOB_PARAMETER lg ON
    (lg.JOB_NAME = jn.JOB_NAME
    AND lg.PARAMETER_NAME = 'JC_JD_LOGON')
LEFT JOIN V_JOB_PARAMETER pf ON
    (pf.JOB_NAME = jn.JOB_NAME
    AND pf.PARAMETER_NAME = 'JC_JD_JOB_PREFIX')
LEFT JOIN V_JOB_PARAMETER tp ON
    (tp.JOB_NAME = jn.JOB_NAME
    AND tp.PARAMETER_NAME = 'JC_TASK_PATH')
LEFT JOIN V_JOB_PARAMETER js ON
    (js.JOB_NAME = jn.JOB_NAME
    AND js.PARAMETER_NAME = 'JC_JS_NAME')
LEFT JOIN V_JOB_PARAMETER rp ON
    (rp.JOB_NAME = jn.JOB_NAME
    AND rp.PARAMETER_NAME = 'JC_REC_ABEND_PROMPT')
LEFT JOIN V_JOB_PARAMETER ra ON
    (ra.JOB_NAME = jn.JOB_NAME
    AND ra.PARAMETER_NAME = 'JC_REC_ACTION')
LEFT JOIN V_JOB_PARAMETER rs ON
    (rs.JOB_NAME = jn.JOB_NAME
    AND rs.PARAMETER_NAME = 'JC_RES_DB2_DEFAULT_VALUE')
CROSS JOIN V_ENVIRONMENT_TENANT et) SELECT
    XMLELEMENT(name "model:TWSBatchApplicationInstance",
    XMLNAMESPACES('http://www.ibm.com/xmlns/prod/scheduling/1.0/Model' AS "model"),
    XMLELEMENT(NAME "model:Name",
    'JS_ETL_NEU'),
    XMLAGG( XMLELEMENT(NAME "model:JobDefinition",
    XMLATTRIBUTES('$' || JC_JD_JOB_PREFIX || '_' || job_name || '$' AS "name",
    JC_JD_LOGON AS "logon",
    JC_JD_FOLDER AS "folder",
    JC_JD_WORKSTATION AS "workstation"),
    XMLELEMENT(NAME "model:task",
    XMLATTRIBUTES(JC_TASK_PATH || job_name || '.sh' AS "exec") ),
    XMLELEMENT(name "model:recovery",
    XMLATTRIBUTES(JC_REC_ABEND_PROMPT AS "abendPrompt",
    JC_REC_ACTION AS "action")) ) ),
    XMLELEMENT(NAME "model:JobStream",
    XMLATTRIBUTES('101' AS "priority" ,
    MIN(JC_JS_NAME) AS "name",
    MIN(JC_JD_FOLDER) AS "folder",
    MIN(JC_JD_WORKSTATION) AS "workstation"),
    XMLELEMENT(NAME "model:matching",
    XMLELEMENT(NAME "model:sameDay") ),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT(NAME "model:dependencies"),
    XMLELEMENT(NAME "model:jobs",
    XMLAGG( XMLELEMENT(NAME "model:job",
    XMLATTRIBUTES( '$JOB_' || job_name || '$' AS "name",
    'false' AS "confirmed",
    'false' AS "isCritical",
    'false' AS "iskey",
    (JC_JD_WORKSTATION || '#' || JC_JD_FOLDER || '$' || JC_JD_JOB_PREFIX || '_' || job_name || '$') AS "defintion",
    '10' AS "priority"),
    XMLELEMENT(NAME "model:resource",
    XMLATTRIBUTES('2' AS "quantity",
    (JC_JD_WORKSTATION || '#' || JC_RES_DB2_DEFAULT_VALUE ) AS "ref")),
    XMLELEMENT(NAME "model:restrictions"),
    XMLELEMENT ( NAME "model:dependencies",
    (
    SELECT
        XMLAGG ( XMLELEMENT ( NAME "model:predecessor",
        XMLATTRIBUTES('$JOB_' || UPPER(v.PARENT_JOB_NAME) || '$' AS "target"),
        XMLELEMENT ( NAME "model:ConditionalDependency",
        XMLELEMENT ( NAME "model:status",
        XMLATTRIBUTES('SUCC' AS "name") ) ) ) )
    FROM
        V_JOB_DEPENDENCIES_FINAL v
    WHERE
        UPPER(v.CHILD_JOB_NAME) = job_par.job_name ) ) ) ) ) ),
    XMLELEMENT(NAME "model:Resource",
    XMLATTRIBUTES(MIN(JC_RES_DB2_DEFAULT_VALUE) AS "name",
    '2' AS "units",
    MIN(JC_JD_WORKSTATION) AS "workstation"))) xml_job
FROM
    job_par
GROUP BY
    TENANT_NAME;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE     VIEW JOB_REPO.v_generated_scripts (job_name,
                                       source_code, starter_source_code) AS
WITH jb (job_name,
         parameter_name,
         value,
         source_code,
         starter_source_code,
         ROW_NUMBER) AS (
    SELECT job_name,
           parameter_name,
           value,
           REPLACE(source_code, '@@@' || parameter_name || '@@@', value),
           REPLACE(starter_source_code, '@@@' || parameter_name || '@@@', value),
           ROW_NUMBER
    FROM JOB_REPO.V_job_parameter
    WHERE ROW_NUMBER = 1
    UNION ALL
    SELECT jb.job_name,
           p.parameter_name,
           p.value,
           REPLACE(jb.source_code, '@@@' || p.parameter_name || '@@@', p.value),
           REPLACE(jb.starter_source_code, '@@@' || p.parameter_name || '@@@', p.value),
           jb.row_number + 1
    FROM jb,
         JOB_REPO.V_job_parameter p
    WHERE p.job_name = jb.job_name
      AND p.row_number = jb.row_number + 1)
SELECT jb.job_name,
       jb.source_code,
       jb.starter_source_code
FROM jb
WHERE ROW_NUMBER IN (
    SELECT MAX(ROW_NUMBER)
    FROM jb i
    WHERE i.job_name = jb.job_name);









SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view V_JOB_PARAMETER_ENV_TEN as
with q1 as (
select e.environment_name,
       e.tenant_name,
       e.tenant_name ||'_' || v.job_name job_name,
       e.PARAMTER_ENVIRONMENT_ID parameter_name,
       e.environment_name        value,
       1                         priority,
       v.SOURCE_CODE,
       v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e),
     q2 as(
select e.environment_name,
       e.tenant_name,
        e.tenant_name ||'_' || v.job_name job_name,
       e.PARAMETER_TENANT_ID parameter_name,
       e.TENANT_NAME         value,
       2                     priority,
       v.SOURCE_CODE,
         v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e),
     q3 as(
select e.environment_name,
       e.tenant_name,
        e.tenant_name ||'_' || v.job_name job_name,
       p.PARAMETER parameter_name,
       p.DATABASE,
       3 priority,
       v.SOURCE_CODE,
         v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e
        join JOB_REPO.database_parameter p on (p.ENVIRONMENT_ID = e.ENVIRONMENT_id and e.TENANT_ID = p.TENANT_ID)),
     uq as (
         select * from q1
         union all
         select * from q2
         union all
         select * from q3
     )
select environment_name, tenant_name, job_name,parameter_name,value, source_code,   starter_source_code, priority,
       row_number() over ( partition by environment_name, tenant_name, job_name order by priority) row_number
       from uq;



SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE     VIEW JOB_REPO.v_generated_scripts_FINAL AS WITH jb (environment_name,
                                                        job_name,
                                                        parameter_name,
                                                        value,
                                                        source_code,
                                                        starter_source_code,
                                                        ROW_NUMBER) AS (
    SELECT environment_name,
           LOWER(job_name) job_name,
           parameter_name,
           value,
           REPLACE(source_code, '@@@' || parameter_name || '@@@', value),
        REPLACE(starter_source_code, '@@@' || parameter_name || '@@@', value),
           ROW_NUMBER
    FROM JOB_REPO.V_job_parameter_env_ten
    WHERE ROW_NUMBER = 1
    UNION ALL
    SELECT jb.environment_name,
           LOWER(jb.job_name) job_name,
           p.parameter_name,
           p.value,
           REPLACE(jb.source_code, '@@@' || p.parameter_name || '@@@', p.value),
            REPLACE(jb.starter_source_code, '@@@' || p.parameter_name || '@@@', p.value),
           jb.row_number + 1
    FROM jb,
         JOB_REPO.V_job_parameter_env_ten p
    WHERE p.job_name = jb.job_name
      AND p.row_number = jb.row_number + 1)
                                               SELECT DISTINCT environment_name,
                                                               job_name,
                                                               source_code,
                                                               starter_source_code
                                               FROM jb
                                               WHERE ROW_NUMBER IN (
                                                   SELECT MAX(ROW_NUMBER)
                                                   FROM jb i
                                                   WHERE i.job_name = jb.job_name
                                                     AND i.environment_name = jb.environment_name);




SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view V_JOB_PARAMETER_ENV_TEN as
with q1 as (
select e.environment_name,
       e.tenant_name,
       e.tenant_name ||'_' || v.job_name job_name,
       e.PARAMTER_ENVIRONMENT_ID parameter_name,
       e.environment_name        value,
       1                         priority,
       v.SOURCE_CODE,
       v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e),
     q2 as(
select e.environment_name,
       e.tenant_name,
        e.tenant_name ||'_' || v.job_name job_name,
       e.PARAMETER_TENANT_ID parameter_name,
       e.TENANT_NAME         value,
       2                     priority,
       v.SOURCE_CODE,
         v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e),
     q3 as(
select e.environment_name,
       e.tenant_name,
        e.tenant_name ||'_' || v.job_name job_name,
       p.PARAMETER parameter_name,
       p.DATABASE,
       3 priority,
       v.SOURCE_CODE,
         v.starter_source_code
from JOB_REPO.v_generated_scripts v
         cross join JOB_REPO.v_environment_tenant e
        join JOB_REPO.database_parameter p on (p.ENVIRONMENT_ID = e.ENVIRONMENT_id and e.TENANT_ID = p.TENANT_ID)),
     uq as (
         select * from q1
         union all
         select * from q2
         union all
         select * from q3
     )
select environment_name, tenant_name, job_name,parameter_name,value, source_code,   starter_source_code, priority,
       row_number() over ( partition by environment_name, tenant_name, job_name order by priority) row_number
       from uq;


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE     VIEW JOB_REPO.v_generated_scripts_FINAL AS WITH jb (environment_name,
                                                        job_name,
                                                        parameter_name,
                                                        value,
                                                        source_code,
                                                        starter_source_code,
                                                        ROW_NUMBER) AS (
    SELECT environment_name,
           LOWER(job_name) job_name,
           parameter_name,
           value,
           REPLACE(source_code, '@@@' || parameter_name || '@@@', value),
        REPLACE(starter_source_code, '@@@' || parameter_name || '@@@', value),
           ROW_NUMBER
    FROM JOB_REPO.V_job_parameter_env_ten
    WHERE ROW_NUMBER = 1
    UNION ALL
    SELECT jb.environment_name,
           LOWER(jb.job_name) job_name,
           p.parameter_name,
           p.value,
           REPLACE(jb.source_code, '@@@' || p.parameter_name || '@@@', p.value),
            REPLACE(jb.starter_source_code, '@@@' || p.parameter_name || '@@@', p.value),
           jb.row_number + 1
    FROM jb,
         JOB_REPO.V_job_parameter_env_ten p
    WHERE p.job_name = jb.job_name
      AND p.row_number = jb.row_number + 1)
                                               SELECT DISTINCT environment_name,
                                                               job_name,
                                                               source_code,
                                                               starter_source_code
                                               FROM jb
                                               WHERE ROW_NUMBER IN (
                                                   SELECT MAX(ROW_NUMBER)
                                                   FROM jb i
                                                   WHERE i.job_name = jb.job_name
                                                     AND i.environment_name = jb.environment_name);






COMMIT WORK;

CONNECT RESET;

TERMINATE;

