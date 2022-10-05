-- This CLP file was created using DB2LOOK Version "11.1" 
-- Timestamp: Sat 01 Aug 2020 02:00:02 AM CEST
-- Database Name: DB01           
-- Database Manager Version: DB2/LINUXX8664 Version 11.1.2.2
-- Database Codepage: 1208
-- Database Collating Sequence is: IDENTITY
-- Alternate collating sequence(alt_collate): null
-- varchar2 compatibility(varchar2_compat): OFF


CONNECT TO DB01;

------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."TABLE_REPO_FOREIGN_KEY"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."TABLE_REPO_FOREIGN_KEY"  (
		  "TABLE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "TABLE_REF_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "FKEY_ID" BOOLEAN NOT NULL WITH DEFAULT true , 
		  "FKEY_SID" BOOLEAN NOT NULL WITH DEFAULT FALSE )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "TABLE_REPO"."TABLE_REPO_FOREIGN_KEY" IS 'Defines foreign key realtionship between tables. Table_id foregin key is in table_ref_id';


-- DDL Statements for Primary Key on Table "TABLE_REPO"."TABLE_REPO_FOREIGN_KEY"

ALTER TABLE "TABLE_REPO"."TABLE_REPO_FOREIGN_KEY" 
	ADD CONSTRAINT "TABLE_REPO_FOREIGN_KEY_PK" PRIMARY KEY
		("TABLE_ID",
		 "TABLE_REF_ID");



------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."TABLE_COLUMN"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."TABLE_COLUMN"  (
		  "TABLE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "SRC_COLUMN_NAME" VARCHAR(100 OCTETS) NOT NULL , 
		  "DWH_COLUMN_NAME" VARCHAR(100 OCTETS) , 
		  "SRC_COLUMN_POS" INTEGER NOT NULL WITH DEFAULT 0 , 
		  "USE_INTERFACE" BOOLEAN NOT NULL WITH DEFAULT TRUE , 
		  "USE_RAW_VAULT" BOOLEAN NOT NULL WITH DEFAULT TRUE , 
		  "USE_CORE" BOOLEAN WITH DEFAULT TRUE , 
		  "SRC_TYPE" VARCHAR(100 OCTETS) , 
		  "SRC_LENGTH" INTEGER , 
		  "SRC_SCALE" INTEGER , 
		  "SRC_NOT_NULL" VARCHAR(1 OCTETS) NOT NULL WITH DEFAULT 'N' , 
		  "CORE_PARTITION_POS" INTEGER , 
		  "CORE_SID_COL" BOOLEAN NOT NULL WITH DEFAULT FALSE , 
		  "CORE_ID_COL" BOOLEAN NOT NULL WITH DEFAULT FALSE , 
		  "CORE_BUCKET_COL" INTEGER , 
		  "CORE_BUCKET_SORT_COL" INTEGER , 
		  "REMARKS" VARCHAR(1000 OCTETS) , 
		  "REMARKS_E" VARCHAR(1000 OCTETS) , 
		  "MESSAGE_FILTER" BOOLEAN NOT NULL WITH DEFAULT FALSE , 
		  "USE_DM" BOOLEAN WITH DEFAULT FALSE )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "TABLE_REPO"."TABLE_COLUMN" IS 'Contains all columns used in tables. Metadata ';


-- DDL Statements for Primary Key on Table "TABLE_REPO"."TABLE_COLUMN"

ALTER TABLE "TABLE_REPO"."TABLE_COLUMN" 
	ADD CONSTRAINT "TABLE_COLUMN_PK" PRIMARY KEY
		("TABLE_ID",
		 "SRC_COLUMN_NAME");



------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."TABLE_SHARED_COLUMN"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."TABLE_SHARED_COLUMN"  (
		  "TABLE_ID" VARCHAR(200 OCTETS) NOT NULL , 
		  "COLUMN_NAME" VARCHAR(100 OCTETS) NOT NULL , 
		  "TABLE_GROUP_ID" VARCHAR(100 OCTETS) , 
		  "COLUMN_POS" INTEGER NOT NULL WITH DEFAULT 0 , 
		  "USE_INTERFACE" BOOLEAN NOT NULL WITH DEFAULT TRUE , 
		  "USE_RAW_VAULT" BOOLEAN NOT NULL WITH DEFAULT TRUE , 
		  "USE_CORE" BOOLEAN WITH DEFAULT TRUE , 
		  "TYPE" VARCHAR(100 OCTETS) , 
		  "LENGTH" INTEGER , 
		  "SCALE" INTEGER , 
		  "CORE_PARTITION_POS" INTEGER , 
		  "CORE_SID_COL" BOOLEAN NOT NULL WITH DEFAULT FALSE , 
		  "CORE_ID_COL" BOOLEAN NOT NULL WITH DEFAULT FALSE , 
		  "REMARKS" VARCHAR(1000 OCTETS) , 
		  "USE_DM" BOOLEAN WITH DEFAULT false )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "TABLE_REPO"."TABLE_SHARED_COLUMN" IS 'Contains additional technical columns used in multiple tables. Table_group_id takes the 5 column_names defined for that specific table_group_id';


-- DDL Statements for Primary Key on Table "TABLE_REPO"."TABLE_SHARED_COLUMN"

ALTER TABLE "TABLE_REPO"."TABLE_SHARED_COLUMN" 
	ADD CONSTRAINT "TABLE_COLUMN_PK" PRIMARY KEY
		("TABLE_ID",
		 "COLUMN_NAME");



------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."TABLE"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."TABLE"  (
		  "TABLE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "SOURCE_SYSTEM" VARCHAR(100 OCTETS) NOT NULL WITH DEFAULT 'Life Factory' , 
		  "TABLE_SCHEMA" VARCHAR(40 OCTETS) NOT NULL , 
		  "TABLE_NAME" VARCHAR(100 OCTETS) NOT NULL , 
		  "USAGE_FLAG" BOOLEAN NOT NULL WITH DEFAULT false , 
		  "DATABASE_RAW_VAULT" VARCHAR(100 OCTETS) , 
		  "DATABASE_CORE" VARCHAR(100 OCTETS) , 
		  "DATABASE_VIEW" VARCHAR(100 OCTETS) , 
		  "FILE_TYPE_RAW_VAULT" VARCHAR(100 OCTETS) NOT NULL WITH DEFAULT 'parquet' , 
		  "FILE_TYPE_CORE" VARCHAR(100 OCTETS) NOT NULL WITH DEFAULT 'parquet' , 
		  "TABLE_GROUP_ID" VARCHAR(100 OCTETS) , 
		  "ID_COLUMN" VARCHAR(100 OCTETS) , 
		  "SID_COLUMN" VARCHAR(100 OCTETS) , 
		  "NUMBER_OF_BUCKETS" INTEGER , 
		  "REMARKS" VARCHAR(2000 OCTETS) , 
		  "REMARKS_EN" VARCHAR(2000 OCTETS) , 
		  "KEY_TABLE" VARCHAR(1000 OCTETS) , 
		  "HIST_TYPE" VARCHAR(100 OCTETS) , 
		  "DATABASE_DATAMART" VARCHAR(100 OCTETS) , 
		  "DATABASE_DM_VIEW" VARCHAR(200 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "TABLE_REPO"."TABLE" IS 'Contains definition of all tables used in the system. Definitions are extracted from db2 catalog of different source systems(lf, sap, bpms). db2 system catalog have all metadata info. This was exported directly int this table';


-- DDL Statements for Primary Key on Table "TABLE_REPO"."TABLE"

ALTER TABLE "TABLE_REPO"."TABLE" 
	ADD CONSTRAINT "TABLE_PK" PRIMARY KEY
		("TABLE_ID");


-- DDL Statements for Unique Constraints on Table "TABLE_REPO"."TABLE"


ALTER TABLE "TABLE_REPO"."TABLE" 
	ADD CONSTRAINT "TABLE_UK" UNIQUE
		("SOURCE_SYSTEM",
		 "TABLE_SCHEMA",
		 "TABLE_NAME");


------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."DATABASES"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."DATABASES"  (
		  "DATABASE" VARCHAR(200 OCTETS) NOT NULL , 
		  "ENVIRONMENT" VARCHAR(100 OCTETS) NOT NULL , 
		  "TENANT_NAME" VARCHAR(200 OCTETS) NOT NULL , 
		  "DATABASE_TARGET" VARCHAR(100 OCTETS) NOT NULL , 
		  "PARAMETER" VARCHAR(100 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "TABLE_REPO"."DATABASES" IS 'Table is not used anymore. ';


-- DDL Statements for Primary Key on Table "TABLE_REPO"."DATABASES"

ALTER TABLE "TABLE_REPO"."DATABASES" 
	ADD CONSTRAINT "DATABASES_PK" PRIMARY KEY
		("DATABASE",
		 "ENVIRONMENT",
		 "TENANT_NAME");



------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."JOIN"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."JOIN"  (
		  "JOIN_ID" VARCHAR(64 OCTETS) NOT NULL , 
		  "DESCRIPTION" VARCHAR(256 OCTETS) , 
		  "SRC_SCHEMA" VARCHAR(100 OCTETS) WITH DEFAULT 'core' , 
		  "JOIN_KEY_TABLE" BOOLEAN WITH DEFAULT false )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "TABLE_REPO"."JOIN" IS 'Reference table for joins between tables';


-- DDL Statements for Primary Key on Table "TABLE_REPO"."JOIN"

ALTER TABLE "TABLE_REPO"."JOIN" 
	ADD CONSTRAINT "JOIN_PK" PRIMARY KEY
		("JOIN_ID");



------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."JOIN_2_TABLE"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."JOIN_2_TABLE"  (
		  "JOIN_ID" VARCHAR(64 OCTETS) NOT NULL , 
		  "TABLE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "ORDER" INTEGER NOT NULL WITH DEFAULT 1 , 
		  "JOIN_TYPE" VARCHAR(20 OCTETS) WITH DEFAULT 'inner join' , 
		  "COL_POS_START" INTEGER WITH DEFAULT 0 )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "TABLE_REPO"."JOIN_2_TABLE" IS 'Defines joins that are used for loading data into the core. Table_id has the table name and join_id has the tables that needs to be joined to table_id. 2 or more tables can be used for joining.';


-- DDL Statements for Primary Key on Table "TABLE_REPO"."JOIN_2_TABLE"

ALTER TABLE "TABLE_REPO"."JOIN_2_TABLE" 
	ADD CONSTRAINT "JOIN_2_TABLE_PK" PRIMARY KEY
		("JOIN_ID",
		 "TABLE_ID");



------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."JOIN_CONDITION"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."JOIN_CONDITION"  (
		  "JOIN_ID" VARCHAR(64 OCTETS) NOT NULL , 
		  "TABLE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "SRC_COLUMN_NAME" VARCHAR(100 OCTETS) NOT NULL , 
		  "JOIN_CONDITION_OFFSET" INTEGER WITH DEFAULT 0 , 
		  "JOIN_TABLE_DATABASE" VARCHAR(100 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "TABLE_REPO"."JOIN_CONDITION" IS 'Defines on what columns we have to define the join condition.For example, slect *from tanb1 join tab2 on tab1.lvid = tab2.lvid';


-- DDL Statements for Primary Key on Table "TABLE_REPO"."JOIN_CONDITION"

ALTER TABLE "TABLE_REPO"."JOIN_CONDITION" 
	ADD CONSTRAINT "JOIN_CONDITION_PK" PRIMARY KEY
		("JOIN_ID",
		 "SRC_COLUMN_NAME",
		 "TABLE_ID");



------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."JOIN_COLUMN"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."JOIN_COLUMN"  (
		  "JOIN_ID" VARCHAR(64 OCTETS) NOT NULL WITH DEFAULT 'test' , 
		  "TABLE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "SRC_COLUMN_NAME" VARCHAR(100 OCTETS) NOT NULL , 
		  "COLUMN_NAME_NEW" VARCHAR(100 OCTETS) , 
		  "COL_POS_OFFSET" INTEGER WITH DEFAULT 0 )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "TABLE_REPO"."JOIN_COLUMN" IS 'Contains the columns  of the join_id that are joined to the target tables ';


-- DDL Statements for Primary Key on Table "TABLE_REPO"."JOIN_COLUMN"

ALTER TABLE "TABLE_REPO"."JOIN_COLUMN" 
	ADD CONSTRAINT "JOIN_COLUMN_PK" PRIMARY KEY
		("JOIN_ID",
		 "SRC_COLUMN_NAME",
		 "TABLE_ID");



------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."TEST"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."TEST"  (
		  "X1" INTEGER , 
		  "X2" INTEGER , 
		  "X3" INTEGER , 
		  "X4" INTEGER , 
		  "X5" INTEGER , 
		  "X6" INTEGER , 
		  "X7" INTEGER , 
		  "X8" INTEGER , 
		  "X9" INTEGER , 
		  "X10" INTEGER , 
		  "X11" INTEGER , 
		  "X12" INTEGER , 
		  "X13" INTEGER , 
		  "X14" INTEGER , 
		  "X15" INTEGER , 
		  "X16" INTEGER )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 

COMMENT ON TABLE "TABLE_REPO"."TEST" IS 'Table not needed';






------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."SMOKE_TABELLE"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."SMOKE_TABELLE"  (
		  "TABLE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "QUELLSYSTEM" VARCHAR(100 OCTETS) NOT NULL , 
		  "TABELLEN_NAME" VARCHAR(100 OCTETS) NOT NULL , 
		  "QUELLSCHEMA" VARCHAR(100 OCTETS) , 
		  "PRIMARY_KEY" VARCHAR(100 OCTETS) , 
		  "DATENSATZ_ID" VARCHAR(100 OCTETS) , 
		  "BESCHREIBUNG_TABELLE" VARCHAR(2000 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 






------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."COG_TEST_TABELLE"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."COG_TEST_TABELLE"  (
		  "TABLE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "QUELLSYSTEM" VARCHAR(100 OCTETS) NOT NULL , 
		  "TABELLEN_NAME" VARCHAR(100 OCTETS) NOT NULL , 
		  "QUELLSCHEMA" VARCHAR(100 OCTETS) , 
		  "PRIMARY_KEY" VARCHAR(100 OCTETS) , 
		  "DATENSATZ_ID" VARCHAR(100 OCTETS) , 
		  "BESCHREIBUNG_TABELLE" VARCHAR(2000 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 






------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."COG_TEST_TABELLE_SPALTE"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."COG_TEST_TABELLE_SPALTE"  (
		  "TABLE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "QUELLSYSTEM" VARCHAR(100 OCTETS) NOT NULL , 
		  "TABELLEN_NAME" VARCHAR(100 OCTETS) NOT NULL , 
		  "QUELLSCHEMA" VARCHAR(100 OCTETS) , 
		  "PRIMARY_KEY" VARCHAR(100 OCTETS) , 
		  "DATENSATZ_ID" VARCHAR(100 OCTETS) , 
		  "BESCHREIBUNG_TABELLE" VARCHAR(2000 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 






------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."TABLE_TEST"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."TABLE_TEST"  (
		  "TABLE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "SOURCE_SYSTEM" VARCHAR(100 OCTETS) NOT NULL WITH DEFAULT 'Life Factory' , 
		  "TABLE_SCHEMA" VARCHAR(40 OCTETS) NOT NULL , 
		  "TABLE_NAME" VARCHAR(100 OCTETS) NOT NULL , 
		  "USAGE_FLAG" BOOLEAN NOT NULL WITH DEFAULT false , 
		  "DATABASE_RAW_VAULT" VARCHAR(100 OCTETS) , 
		  "DATABASE_CORE" VARCHAR(100 OCTETS) , 
		  "DATABASE_VIEW" VARCHAR(100 OCTETS) , 
		  "FILE_TYPE_RAW_VAULT" VARCHAR(100 OCTETS) NOT NULL WITH DEFAULT 'parquet' , 
		  "FILE_TYPE_CORE" VARCHAR(100 OCTETS) NOT NULL WITH DEFAULT 'parquet' , 
		  "TABLE_GROUP_ID" VARCHAR(100 OCTETS) , 
		  "ID_COLUMN" VARCHAR(100 OCTETS) , 
		  "SID_COLUMN" VARCHAR(100 OCTETS) , 
		  "NUMBER_OF_BUCKETS" INTEGER , 
		  "REMARKS" VARCHAR(2000 OCTETS) , 
		  "REMARKS_EN" VARCHAR(2000 OCTETS) , 
		  "KEY_TABLE" VARCHAR(1000 OCTETS) , 
		  "HIST_TYPE" VARCHAR(100 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 






------------------------------------------------
-- DDL Statements for Table "TABLE_REPO"."TABLE_COLUMN_TEST"
------------------------------------------------
 

CREATE TABLE "TABLE_REPO"."TABLE_COLUMN_TEST"  (
		  "TABLE_ID" VARCHAR(100 OCTETS) NOT NULL , 
		  "SRC_COLUMN_NAME" VARCHAR(100 OCTETS) NOT NULL , 
		  "DWH_COLUMN_NAME" VARCHAR(100 OCTETS) , 
		  "SRC_COLUMN_POS" INTEGER NOT NULL WITH DEFAULT 0 , 
		  "USE_INTERFACE" BOOLEAN NOT NULL WITH DEFAULT TRUE , 
		  "USE_RAW_VAULT" BOOLEAN NOT NULL WITH DEFAULT TRUE , 
		  "USE_CORE" BOOLEAN WITH DEFAULT TRUE , 
		  "SRC_TYPE" VARCHAR(100 OCTETS) , 
		  "SRC_LENGTH" INTEGER , 
		  "SRC_SCALE" INTEGER , 
		  "SRC_NOT_NULL" VARCHAR(1 OCTETS) NOT NULL WITH DEFAULT 'N' , 
		  "CORE_PARTITION_POS" INTEGER , 
		  "CORE_SID_COL" BOOLEAN NOT NULL WITH DEFAULT FALSE , 
		  "CORE_ID_COL" BOOLEAN NOT NULL WITH DEFAULT FALSE , 
		  "CORE_BUCKET_COL" INTEGER , 
		  "CORE_BUCKET_SORT_COL" INTEGER , 
		  "REMARKS" VARCHAR(1000 OCTETS) , 
		  "REMARKS_E" VARCHAR(1000 OCTETS) , 
		  "MESSAGE_FILTER" BOOLEAN NOT NULL WITH DEFAULT FALSE )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW; 






-- DDL Statements for Foreign Keys on Table "TABLE_REPO"."JOIN_2_TABLE"

ALTER TABLE "TABLE_REPO"."JOIN_2_TABLE" 
	ADD CONSTRAINT "JIJOIN_2_TABLE___FK" FOREIGN KEY
		("JOIN_ID")
	REFERENCES "TABLE_REPO"."JOIN"
		("JOIN_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

ALTER TABLE "TABLE_REPO"."JOIN_2_TABLE" 
	ADD CONSTRAINT "JOIN_2_TABLE_TABLE_TABLE_ID_FK" FOREIGN KEY
		("TABLE_ID")
	REFERENCES "TABLE_REPO"."TABLE"
		("TABLE_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

-- DDL Statements for Foreign Keys on Table "TABLE_REPO"."JOIN_CONDITION"

ALTER TABLE "TABLE_REPO"."JOIN_CONDITION" 
	ADD CONSTRAINT "JOIN_CONDITION_JOIN_JOIN_ID_FK" FOREIGN KEY
		("JOIN_ID")
	REFERENCES "TABLE_REPO"."JOIN"
		("JOIN_ID")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

ALTER TABLE "TABLE_REPO"."JOIN_CONDITION" 
	ADD CONSTRAINT "JOIN_CONDITION_TABLE_COLUMN_TABLE_ID_SRC_COLUMN_NAME_FK" FOREIGN KEY
		("TABLE_ID",
		 "SRC_COLUMN_NAME")
	REFERENCES "TABLE_REPO"."TABLE_COLUMN"
		("TABLE_ID",
		 "SRC_COLUMN_NAME")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;

-- DDL Statements for Foreign Keys on Table "TABLE_REPO"."JOIN_COLUMN"

ALTER TABLE "TABLE_REPO"."JOIN_COLUMN" 
	ADD CONSTRAINT "JOIN_COLUMN_TABLE_COLUMN_TABLE_ID_SRC_COLUMN_NAME_FK" FOREIGN KEY
		("TABLE_ID",
		 "SRC_COLUMN_NAME")
	REFERENCES "TABLE_REPO"."TABLE_COLUMN"
		("TABLE_ID",
		 "SRC_COLUMN_NAME")
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	ENFORCED
	ENABLE QUERY OPTIMIZATION;






----------------------------

-- DDL Statements for Views

----------------------------
SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.v_docu_raw_vault as
with s1 as (
    select t.DATABASE_RAW_VAULT DATABASE,
           lower(t.TABLE_NAME) TABLE,
           c.COLUMN,
           ROW_NUMBER() OVER (partition by table_name order by SRC_COLUMN_POS  ) COLUMN_POS,
           c.DWH_TYPE TYPE,
           c.DWH_LENGTH LENGTH,
           c.DWH_SCALE SCALE,
           c.REMARKS REMARKS
    from TABLE_REPO.TABLE t
             join TABLE_REPO.V_TABLE_COLUMN_ALL c on (c.table_id = t.table_id)
    where t.USAGE_FLAG = true
      and c.USE_RAW_VAULT = true

)
select *
from s1;


COMMENT ON TABLE "TABLE_REPO"."V_DOCU_RAW_VAULT" IS 'Contains definition of all raw_zone tables. All these defintions are extracted from db2 catalog';


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view table_repo.v_docu_tables as
select  lower(source_system || '_' || table_name) as Entitätstyp, nvl(remarks,'') as Beschreibung from table_repo.table;


COMMENT ON TABLE "TABLE_REPO"."V_DOCU_TABLES" IS 'We dont need this view';


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.v_docu_interface as
with s1 as (
    select t.TABLE_SCHEMA SCHEMA_QUELLSYSTEM,
           t.TABLE_NAME TABELLENNAME,
           COLUMN_NAME SPALTENNAME,
           c.SRC_COLUMN_POS SPALTENPOSITION,
           c.SRC_TYPE DATENTYP,
           c.SRC_LENGTH LAENGE,
           c.SRC_SCALE SCALE,
           c.SRC_NOT_NULL NULLABLE,
           c.REMARKS BESCHREIBUNG
    from TABLE_REPO.TABLE t
             join TABLE_REPO.V_TABLE_COLUMN_ALL c on (c.table_id = t.table_id)
    where t.USAGE_FLAG = true
      and c.USE_INTERFACE = true
)
select *
from s1;


COMMENT ON TABLE "TABLE_REPO"."V_DOCU_INTERFACE" IS 'Provides docu about data provided by source system to be used in confluence. 
		Defines all fields of source system tables used in thw dwh. Doesnot have any technical fields';


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW table_repo.v_table_foreign_key_parameter AS
    WITH sid_json AS (
SELECT
    c.source_system || '_' || c.TABLE_NAME table_name_dwh,
    c.DATABASE_CORE,
    d.SID_COLUMN,
    a.table_id,
    a.TABLE_REF_ID,
    'FOREIGN_KEY' PARAMETER,
    '"' || LOWER(d.SID_COLUMN) || '":["' || listagg(e.column_name,
    '","') WITHIN GROUP (
ORDER BY
    e.COLUMN_NAME) || '"]' STR_JSON
FROM
    table_repo.TABLE_REPO_FOREIGN_KEY a
left JOIN table_repo."TABLE" c ON
    (c.TABLE_ID = a.TABLE_ID)
left JOIN table_repo.TABLE d ON
    (d.TABLE_ID = a.TABLE_REF_ID)
left  JOIN  table_repo.V_TABLE_COLUMN_ALL e on
    (a.table_ref_id = e.table_id)
WHERE
    e.core_sid_col IS TRUE
    and a.fkey_sid = true
GROUP BY
    c.source_system || '_' || c.TABLE_NAME,
    c.DATABASE_CORE,
    d.SID_COLUMN,
          a.table_id,
    a.TABLE_REF_ID ),
id_json AS (
SELECT
    c.source_system || '_' || c.TABLE_NAME table_name_dwh,
    c.DATABASE_CORE,
    d.ID_COLUMN,
    a.table_id,
    a.TABLE_REF_ID,
    'FOREIGN_KEY' PARAMETER,
    '"' || LOWER(d.ID_COLUMN) || '":["' || listagg(e.column_name,
    '","') WITHIN GROUP (
ORDER BY
    e.COLUMN_NAME) || '"]' STR_JSON
FROM
    table_repo.TABLE_REPO_FOREIGN_KEY a
left JOIN table_repo."TABLE" c ON
    (c.TABLE_ID = a.TABLE_ID)
left JOIN table_repo.TABLE d ON
    (d.TABLE_ID = a.TABLE_REF_ID)
left  JOIN  table_repo.V_TABLE_COLUMN_ALL e on
    (a.table_ref_id = e.table_id)
WHERE
    e.core_id_col IS TRUE
    and a.fkey_id = true
GROUP BY
    c.source_system || '_' || c.TABLE_NAME,
    c.DATABASE_CORE,
    d.ID_COLUMN,
          a.table_id,
    a.TABLE_REF_ID),
agg_all AS (
SELECT
    table_name_dwh,
    database_core,
    str_json
FROM
    id_json
UNION ALL
SELECT
    table_name_dwh,
    database_core,
    str_json
FROM
    sid_json )
SELECT
    table_name_dwh,
    database_core,
    'FOREIGN_KEY' PARAMETER,
    listagg(CAST(str_json AS VARCHAR(30000)),
    ',') WITHIN GROUP (
ORDER BY
    1) str_json
FROM
    agg_all a
GROUP BY
    table_name_dwh,
    database_core;


COMMENT ON TABLE "TABLE_REPO"."V_TABLE_FOREIGN_KEY_PARAMETER" IS 'Returns json definition of foregin key relationships for defining paraemters in job_repo. 
		For example, key_type_id and key_type_sid in v_job_parameter takes the values set in v_table_FOREIGN_KEY_PARAMETER';


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view v_ddl_core_key as
with tab_col_all as (
    select key_table,
           column_name,
           ddl_line,
           DATABASE_CORE,
           CORE_PARTITION_POS,
           FIRST_VALUE(src_column_pos) OVER (PARTITION BY KEY_TABLE ORDER BY SRC_COLUMN_POS) col_pos
    from V_TABLE_COLUMN_ALL
    where core_id_col is true
      and KEY_TABLE is not null
      and key_table not like ''
    union
    select key_table, column_name, ddl_line, DATABASE_CORE, core_partition_pos, src_column_pos col_pos
    from V_TABLE_COLUMN_ALL o
    where key_table is not null
      and key_table not like ''
      and exists(select * from TABLE_SHARED_COLUMN i where lower(i.column_name) = lower(o.column_name))),
     agg_fields as (
         select key_table,
                DATABASE_CORE,
                ' (' || listagg(ddl_line, ', ') within group (order by col_pos ) || ') ' fields
         from tab_col_all
         where CORE_PARTITION_POS is null
         group by key_table, database_core
     ),
     agg_part_fields as (
         select key_table,
                DATABASE_CORE,
                ' (' || listagg(ddl_line, ', ') within group (order by col_pos ) || ') ' agg_fields
         from tab_col_all
         where CORE_PARTITION_POS is not null
         group by key_table, database_core
     ),
     create_stmt as (
         select af.key_table,
                af.database_core,
                af.fields,
                apf.agg_fields,
                'CREATE TABLE ' || af.database_core || '.' || af.key_table || af.fields || ' PARTITIONED BY ' ||
                agg_fields as ddl_stmt
         from agg_fields af
                  join agg_part_fields apf on (af.KEY_TABLE = apf.key_table)
     )

select *
from create_stmt;


COMMENT ON TABLE "TABLE_REPO"."V_DDL_CORE_KEY" IS 'Contains ddl stmts for creating key tables';


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view v_cognos_tabllen as
select source_system Quellsystem, table_name Tabellenname, database_view Quellschema, sid_column Primary_Key, id_column Datensatz_id, remarks Beschreibung  from table;



SET CURRENT SCHEMA = "DB2INST1";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE 
VIEW table_repo.v_table_join AS WITH new_columns_list AS(
SELECT
    jc.join_id,
    jc.table_id,
    j.src_schema,
    '["' || jc.src_column_name || '","' || nvl(jc.column_name_new,
    jc.src_column_name) || '"]' new_cols
FROM
    table_repo.JOIN_COLUMN jc
JOIN table_repo."JOIN" j ON
    (jc.join_id = j.join_id)
JOIN table_repo."TABLE" t ON
    (jc.table_id = t.table_id)
GROUP BY
    jc.join_id,
    jc.table_id,
    jc.SRC_COLUMN_NAME,
    jc.COLUMN_NAME_NEW,
    j.src_schema),
new_columns AS (
SELECT
    join_id,
    table_id,
    '"join_schema":"' || src_schema || '"' join_schema,
    '"new_columns":[' || listagg(new_cols,
    ',') WITHIN GROUP(
ORDER BY
    join_id) || ']' new_columns_string
FROM
    new_columns_list
GROUP BY
    join_id,
    table_id,
    src_schema),
columns_name AS (
SELECT
	j2.table_id,
	v.table_name_dwh
FROM 
	table_repo.join_2_table j2
JOIN table_repo.v_table_column_all v ON
	(j2.table_id = v.table_id)
GROUP BY
	j2.table_id,
	v.table_name_dwh
),
table_name AS (
SELECT
    jc.join_id,
    '"joined_table":"' || v.table_name_dwh || '"' name_dwh
FROM
    table_repo.join_condition jc
JOIN table_repo.v_table_column_all v ON
    (v.table_id = jc.table_id)
GROUP BY
    jc.join_id,
    v.table_name_dwh ),
join_condition_2 AS (
SELECT
    join_id,
    table_id,
    '"join_columns":["' || listagg(SRC_COLUMN_NAME,
    '","') WITHIN GROUP (
ORDER BY
    join_id) || '"]' join_condition_string
FROM
    table_repo.JOIN_CONDITION
GROUP BY
    join_id,
    table_id ),
agg_prepare AS (
SELECT
    jc.join_id,
    c.table_name_dwh,
    jc.join_condition_string,
    nc.new_columns_string,
    nc.join_schema,
    tn.name_dwh,
    '"join_type":"' || j2.join_type || '"' join_type
FROM
    join_condition_2 jc
JOIN table_repo.join_2_table j2 ON
    (jc.join_id = j2.join_id)
JOIN new_columns nc ON
    (jc.join_id = nc.join_id)
JOIN table_repo."JOIN" j ON
    (jc.join_id = j.join_id)
JOIN table_name tn ON
    (jc.join_id = tn.join_id)
JOIN columns_name c ON
	(j2.table_id = c.table_id)),
str_cte AS (
SELECT
    table_name_dwh,
    '{' || join_schema || ',' || name_dwh || ',' || join_condition_string || ',' || new_columns_string || ',' || join_type || '}' join_string
FROM
    agg_prepare )
SELECT
    table_name_dwh,
    'core' database_name,
    'JOIN_TABLE' PARAMETER,
    listagg(CAST(LOWER(join_string) AS VARCHAR(3000)),
    ',') WITHIN GROUP (
ORDER BY
    table_name_dwh) string_json
FROM
    str_cte
GROUP BY
    table_name_dwh;


COMMENT ON TABLE "TABLE_REPO"."V_TABLE_JOIN" IS 'Same as V_TABLE_FOREIGN_KEY_PARAMETER but with join condition for job_repo ';


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW TABLE_REPO.v_table_key_parameter AS WITH fields_sid AS (
SELECT
    t.DATABASE_CORE DATABASE,
    t.source_system || '_' || t.TABLE_NAME TABLE,
    'SID' keytype,
    'KEY_TYPE_SID' PARAMETER,
    '"sid":{"field_name":"' || LOWER(t.SID_COLUMN) || '","fields":["' || LISTAGG(V.COLUMN_NAME,
    '","') WITHIN GROUP (
ORDER BY
    V.COLUMN_NAME ) || '"]}' KEY
FROM
    TABLE_REPO.V_TABLE_COLUMN_ALL V
JOIN table_repo.table t ON
    (t.table_id = v.table_id)
WHERE
    V.CORE_SID_COL = TRUE
GROUP BY
     t.source_system || '_' || t.TABLE_NAME,
    t.SID_COLUMN,
    t.DATABASE_CORE),
fields_id AS (
SELECT
    t.DATABASE_CORE DATABASE,
     t.source_system || '_' || t.TABLE_NAME TABLE,
    'ID' keytype,
    'KEY_TYPE_ID' PARAMETER,
    '"id":{"field_name":"' || LOWER(t.ID_COLUMN) || '","fields":["' || LISTAGG(V.COLUMN_NAME,
    '","') WITHIN GROUP (
ORDER BY
    V.COLUMN_NAME ) || '"]}' KEY
FROM
    TABLE_REPO.V_TABLE_COLUMN_ALL V
JOIN table_repo.table t ON
    (t.table_id = v.table_id)
WHERE
    V.CORE_ID_COL = TRUE
GROUP BY
    t.source_system || '_' || t.TABLE_NAME,
    t.ID_COLUMN,
    t.DATABASE_CORE)
SELECT
    *
FROM
    fields_id
UNION ALL
SELECT
    *
FROM
    fields_sid;


COMMENT ON TABLE "TABLE_REPO"."V_TABLE_KEY_PARAMETER" IS 'keys definition for job_repo';


SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW table_repo.V_TABLE_DB2_EXTRACTION AS SELECT
    LOWER(t.source_system || '_' || t.TABLE_NAME) AS TABLE_NAME,
    t.DATABASE_RAW_VAULT AS DATABASE_RAW_ZONE,
    t.DATABASE_CORE AS DATABASE_CORE,
    'select_stmt_db2_source' AS PARAMETER_NAME,
    'select ' || LISTAGG(LOWER(tc.src_column_name),
    ', ') WITHIN GROUP (
ORDER BY
    tc.src_column_name) || ' from ' || t.DATABASE_RAW_VAULT || '.' || LOWER(t.source_system || '_' || t.TABLE_NAME) AS STR_JSON
   
FROM
    TABLE_REPO.table t
JOIN table_repo.table_column tc ON
    (t.table_id = tc.table_id)
WHERE
    tc.USE_INTERFACE = TRUE
GROUP BY
    t.DATABASE_RAW_VAULT,
    t.source_system || '_' || t.TABLE_NAME,
     t.DATABASE_CORE;


COMMENT ON TABLE "TABLE_REPO"."V_TABLE_DB2_EXTRACTION" IS 'Still in construction. View has SQL stmts required for extracting data FROM db2 sources. Used for defining parameters in job_repo';


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE 
VIEW TABLE_REPO.ztest_table_column_all AS WITH src_row AS (
SELECT
    a.TABLE_ID,
    LOWER(nvl(a.DWH_COLUMN_NAME, SRC_COLUMN_NAME)) COLUMN_NAME,
    a.SRC_COLUMN_POS,
    a.USE_INTERFACE,
    a.USE_RAW_VAULT,
    a.USE_CORE,
    a.SRC_TYPE,
    a.SRC_LENGTH,
    a.SRC_SCALE,
    a.SRC_NOT_NULL,
    a.CORE_SID_COL,
    a.CORE_ID_COL,
    a.DWH_COLUMN_NAME,
    a.CORE_PARTITION_POS,
    a.REMARKS
FROM
    TABLE_REPO.TABLE_COLUMN a
JOIN TABLE_REPO.TABLE b ON
    (a.TABLE_ID = b.TABLE_ID)
WHERE
    b.USAGE_FLAG = TRUE),
share_col AS (
SELECT
    a.TABLE_ID,
    LOWER(b.COLUMN_NAME) COLUMN_NAME,
    b.COLUMN_POS,
    b.USE_INTERFACE,
    b.USE_RAW_VAULT,
    b.USE_CORE,
    b.TYPE SRC_TYPE,
    b.LENGTH SRC_LENGTH,
    b.SCALE SRC_SCALE,
    NULL SRC_NOT_NULL,
    b.CORE_SID_COL,
    b.CORE_ID_COL,
    NULL DWH_COLUMN_NAME,
    b.CORE_PARTITION_POS,
    b.REMARKS
FROM
    TABLE_REPO.TABLE_SHARED_COLUMN b
JOIN TABLE_REPO.TABLE a ON
    (a.TABLE_GROUP_ID = b.TABLE_GROUP_ID)
WHERE
    a.USAGE_FLAG = TRUE),
sid_col AS (
SELECT
    a.table_id,
    LOWER(a.SID_COLUMN) COLUMN_NAME,
    0 SRC_COLUMN_POS,
    FALSE USE_INTERFACE,
    FALSE USE_RAW_VAULT,
    TRUE USE_CORE,
    'STRING' SRC_TYPE,
    NULL SRC_LENGTH,
    NULL SRC_SCALE,
    NULL SRC_NOT_NULL,
    FALSE CORE_SID_COL,
    FALSE CORE_ID_COL,
    NULL DWH_COLUMN_NAME,
    NULL CORE_PARTITION_POS,
    'Surrgate Key' REMARKS
FROM
    TABLE_REPO.TABLE a
WHERE
    a.USAGE_FLAG = TRUE),
id_col AS (
SELECT
    a.table_id,
    LOWER(a.ID_COLUMN) COLUMN_NAME,
    1 SRC_COLUMN_POS,
    FALSE USE_INTERFACE,
    FALSE USE_RAW_VAULT,
    TRUE USE_CORE,
    'STRING' SRC_TYPE,
    NULL SRC_LENGTH,
    NULL SRC_SCALE,
    NULL SRC_NOT_NULL,
    FALSE CORE_SID_COL,
    FALSE CORE_ID_COL,
    NULL DWH_COLUMN_NAME,
    NULL CORE_PARTITION_POS,
    'Foreign Key Key table' REMARKS
FROM
    TABLE_REPO.TABLE a
WHERE
    a.USAGE_FLAG = TRUE),
fk_id_col AS (
SELECT
    a.table_id,
    LOWER(c.ID_COLUMN) COLUMN_NAME,
    20 SRC_COLUMN_POS,
    FALSE USE_INTERFACE,
    FALSE USE_RAW_VAULT,
    TRUE USE_CORE,
    'STRING' SRC_TYPE,
    NULL SRC_LENGTH,
    NULL SRC_SCALE,
    NULL SRC_NOT_NULL,
    FALSE CORE_SID_COL,
    FALSE CORE_ID_COL,
    NULL DWH_COLUMN_NAME,
    NULL CORE_PARTITION_POS,
    'Foreign Key' REMARKS
FROM
    TABLE_REPO.TABLE a
JOIN TABLE_REPO.TABLE_REPO_FOREIGN_KEY b ON
    (a.TABLE_ID = b.TABLE_ID)
JOIN TABLE_REPO.TABLE c ON
    (b.TABLE_REF_ID = c.TABLE_ID)
WHERE
    a.USAGE_FLAG = TRUE),
fk_sid_col AS (
SELECT
    a.table_id,
    LOWER(c.SID_COLUMN) COLUMN_NAME,
    10 SRC_COLUMN_POS,
    FALSE USE_INTERFACE,
    FALSE USE_RAW_VAULT,
    TRUE USE_CORE,
    'STRING' SRC_TYPE,
    NULL SRC_LENGTH,
    NULL SRC_SCALE,
    NULL SRC_NOT_NULL,
    FALSE CORE_SID_COL,
    FALSE CORE_ID_COL,
    NULL DWH_COLUMN_NAME,
    NULL CORE_PARTITION_POS,
    'Foreign Key' REMARKS
FROM
    TABLE_REPO.TABLE a
JOIN TABLE_REPO.TABLE_REPO_FOREIGN_KEY b ON
    (a.TABLE_ID = b.TABLE_ID)
JOIN TABLE_REPO.TABLE c ON
    (b.TABLE_REF_ID = c.TABLE_ID)
WHERE
    a.USAGE_FLAG = TRUE
    AND b.FKEY_SID = TRUE),
join_columns AS (
SELECT
    t.table_id,
    c.table_id joined_table_id,
    j.join_id,
    c.src_column_name,
    nvl(c.column_name_new,
    c.src_column_name) column_name_new,
    COL_POS_START,
    col_pos_offset
FROM
    join_2_table t
JOIN JOIN j ON
    (t.join_id = j.join_id)
JOIN join_column c ON
    (c.join_id = t.JOIN_ID) ),
all_columns AS (
SELECT
    *
FROM
    fk_sid_col
UNION ALL
SELECT
    *
FROM
    fk_id_col
UNION ALL
SELECT
    *
FROM
    id_col
UNION ALL
SELECT
    *
FROM
    sid_col
UNION ALL
SELECT
    *
FROM
    src_row
UNION ALL
SELECT
    *
FROM
    share_col ),
add_dwh_type AS (
SELECT
    a.*,
    LOWER(CASE WHEN src_type = 'VARCHAR' THEN 'STRING' WHEN src_type = 'DATE' THEN 'TIMESTAMP' WHEN src_type = 'CHARACTER' THEN 'STRING' WHEN src_type = 'CHAR' THEN 'STRING' WHEN src_type = 'CLNT' THEN 'STRING' WHEN src_type = 'NUMC' THEN 'STRING' WHEN src_type = 'DATS' THEN 'STRING' WHEN src_type = 'TIMS' THEN 'STRING' WHEN src_type = 'RAW' THEN 'STRING' WHEN src_type = 'LANG' THEN 'STRING' WHEN src_type = 'DEC' THEN 'DECIMAL' ELSE src_type END) dwh_type,
    CASE WHEN src_type = 'DECIMAL' THEN SRC_LENGTH
    ELSE NULL
END DWH_LENGTH,
CASE WHEN src_type = 'DECIMAL' THEN SRC_scale
ELSE NULL
END DWH_SCALE
FROM
all_columns a),
create_col_stmt AS (
SELECT
    a.*,
    LOWER(nvl(a.DWH_COLUMN_NAME, a.COLUMN_NAME)) || '  ' || UPPER(a.DWH_TYPE) ||
    CASE WHEN a.DWH_LENGTH IS NOT NULL THEN '(' || TRIM(a.DWH_LENGTH) || ',' || TRIM(a.DWH_SCALE) || ')'
    ELSE ''
END ddl_line,
LOWER(nvl(DWH_COLUMN_NAME, COLUMN_NAME)) COLUMN
FROM
add_dwh_type a)
SELECT * FROM CREATE_COL_STMT;


COMMENT ON TABLE "TABLE_REPO"."ZTEST_TABLE_COLUMN_ALL" IS 'To be deleted';


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.V_DDL_CORE_ACCESS_VIEW as
with v_table_column_all_distinct as (select distinct table_id, table_name_dwh, column_name,remarks, src_column_pos, ddl_line, core_partition_pos, use_core from TABLE_REPO.v_table_column_all),
     access_view as (select v.table_id,
                            'CREATE OR REPLACE VIEW ' || t.DATABASE_VIEW || '.v_' || lower(t.source_system || '_' || t.TABLE_NAME) ||
                            ' AS SELECT ' ||
                            listagg(lower(nvl(v.COLUMN_NAME, v.COLUMN_NAME)), ',')
                                    within group ( order by SRC_COLUMN_POS) || ' FROM ' || t.DATABASE_CORE || '.' ||
                            lower(t.source_system || '_' || t.TABLE_NAME || ';') ddl
                     from v_table_column_all_distinct v
                              join table_repo.table t on (t.table_id = v.table_id)
                     where use_core = true
                     group by v.table_id, t.DATABASE_CORE, t.TABLE_NAME,t.DATABASE_VIEW, t.SOURCE_SYSTEM
)
select *
from access_view a;


COMMENT ON TABLE "TABLE_REPO"."V_DDL_CORE_ACCESS_VIEW" IS 'provides access views FOR data mart tables. This VIEW takes all columns from core tables. Access views are used by end users for accesing data mart tables. This is due to security reasons';


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.V_DDL_CORE_HIST as
   with  v_table_column_all_distinct as (select distinct table_id, column_name,table_name_dwh, remarks, src_column_pos, ddl_line, core_partition_pos, use_core from TABLE_REPO.v_table_column_all),
pas_access_view_hist1 as (select v.table_id,
                            'CREATE OR REPLACE VIEW ' || t.DATABASE_VIEW || '.v_' ||
                            lower(t.source_system || '_' || t.TABLE_NAME) ||
                            '_hist AS SELECT ' ||
                            listagg(lower(nvl(v.COLUMN_NAME, v.COLUMN_NAME)), ',')
                                    within group ( order by SRC_COLUMN_POS)
                                 || ', nvl(lead(gueltig_ab) over(partition by lvid order by bearbid),cast (''2099-12-31 23:59:59'' as timestamp)) gueltig_bis '
                                || ', nvl(lead(wirksam_ab) over(partition by lvid order by bearbid),cast (''2099-12-31 23:59:59'' as timestamp)) wirksam_bis '
                                || ', nvl(lead(insert_tst) over(partition by lvid order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) insert_tst_bis '
                                || ' FROM ' || t.DATABASE_CORE || '.' ||
                            lower(t.source_system || '_' || t.TABLE_NAME || ';') ddl
                     from v_table_column_all_distinct v
                              join table_repo.table t on (t.table_id = v.table_id)
                     where use_core = true
                       and exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'gueltig_ab')
                       and exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'wirksam_ab')
                        and exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'bearbid')
                     group by v.table_id, t.DATABASE_CORE, t.TABLE_NAME, t.DATABASE_VIEW, t.source_system
),
     pas_access_view_hist2 as (select v.table_id,
                            'CREATE OR REPLACE VIEW ' || t.DATABASE_VIEW || '.v_' ||
                            lower(t.source_system || '_' || t.TABLE_NAME) ||
                            '_hist AS SELECT ' ||
                            listagg(lower(nvl(v.COLUMN_NAME, v.COLUMN_NAME)), ',')
                                    within group ( order by SRC_COLUMN_POS)
                                || ', insert_tst wirksam_ab '
                                || ', insert_tst gueltig_ab '
                               || ', nvl(lead(insert_tst) over(partition by lvid order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) gueltig_bis '
                                || ', nvl(lead(insert_tst) over(partition by lvid order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) wirksam_bis '
                                || ', nvl(lead(insert_tst) over(partition by lvid order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) insert_tst_bis '
                                || ' FROM ' || t.DATABASE_CORE || '.' ||
                            lower(t.source_system || '_' || t.TABLE_NAME || ';') ddl
                     from v_table_column_all_distinct v
                              join table_repo.table t on (t.table_id = v.table_id)
                     where use_core = true
                       and not exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'gueltig_ab')
                       and not exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'wirksam_ab')
                        and not exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'bearbid')
                     group by v.table_id, t.DATABASE_CORE, t.TABLE_NAME, t.DATABASE_VIEW, t.source_system
)
select table_id, ddl
from pas_access_view_hist1
union all select table_id, ddl
from pas_access_view_hist2;


COMMENT ON TABLE "TABLE_REPO"."V_DDL_CORE_HIST" IS 'Adds temporara atteibutes to access views For ex: valid_from / valid_to columns';


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW table_repo.v_ddl_raw_vault AS WITH COLUMN AS (
SELECT
    v.TABLE_ID,
    v.TABLE_NAME_DWH,
    ' (' || listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
    CASE
        WHEN REMARKS IS NOT NULL THEN ' COMMENT '''
        ELSE ''
    END || nvl(REMARKS,
    '') ||
    CASE
        WHEN REMARKS IS NOT NULL THEN ''''
        ELSE ''
    END,
    ', ') WITHIN GROUP (
ORDER BY
    SRC_COLUMN_POS) || ') ' lstD
FROM
    TABLE_REPO.v_table_column_all v
WHERE
    v.CORE_PARTITION_POS IS NULL
    AND v.USE_RAW_VAULT = TRUE
GROUP BY
    TABLE_ID,
    TABLE_NAME_DWH),
PARTITION AS (
SELECT
    v.TABLE_ID,
    ' PARTITIONED BY (' || listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
    CASE
        WHEN REMARKS IS NOT NULL THEN ' COMMENT '''
        ELSE ''
    END || nvl(REMARKS,
    '') ||
    CASE
        WHEN REMARKS IS NOT NULL THEN ''''
        ELSE ''
    END,
    ', ') WITHIN GROUP (
ORDER BY
    CORE_PARTITION_POS) || ') ' lst
FROM
    TABLE_REPO.v_table_column_all v
WHERE
    v.CORE_PARTITION_POS IS NOT NULL
GROUP BY
    TABLE_ID ),
head AS (
SELECT
    t.table_id,
    'CREATE TABLE IF NOT EXISTS  ' || t.DATABASE_RAW_VAULT || '.' || LOWER(t.source_system || '_' || t.TABLE_NAME) head
FROM
    table_repo.table t ),
tail AS (
SELECT
    table_id,
    ' STORED AS ' || t.FILE_TYPE_RAW_VAULT || ' TBLPROPERTIES (''PARQUET.COMPRESS''=''SNAPPY'')' tail
FROM
    table_repo.table t),
COMMENT AS (
SELECT
    table_id,
    CASE
        WHEN t.remarks IS NOT NULL
        AND LENGTH(t.remarkS) > 0 THEN ' COMMENT ''' || t.REMARKs || ''''
        ELSE ''
    END COMMENT
FROM
    table_repo.table t )
SELECT
    a.table_id,
    b.table_name_dwh,
    a.head,
    b.lstD,
    c.lst,
    t.tail,
    e.comment,
    a.head || b.lstD || e.comment || c.lst || t.tail || ';' ddl
FROM
    head a
LEFT JOIN COLUMN b ON
    (a.table_id = b.table_id)
LEFT JOIN PARTITION c ON
    (c.TABLE_ID = a.table_id)
LEFT JOIN tail t ON
    (t.table_id = a.table_id)
LEFT JOIN COMMENT e ON
    (e.table_id = a.table_id);



SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW TABLE_REPO.V_DDL_CORE_HIST_SAP AS WITH v_table_column_all_distinct
AS (
SELECT
    DISTINCT table_id,
    column_name,
    table_name_dwh,
    remarks,
    src_column_pos,
    ddl_line,
    core_partition_pos,
    use_core
FROM
    TABLE_REPO.v_table_column_all
WHERE
	TABLE_NAME_DWH LIKE 'PAR%'),
par_access_view_hist1 AS (
SELECT
    v.table_id,
    'CREATE OR REPLACE VIEW ' || t.DATABASE_VIEW || '.v_' || lower(t.source_system || '_' || t.TABLE_NAME) || '_hist AS SELECT ' || listagg(lower(nvl(v.COLUMN_NAME, v.COLUMN_NAME)),
    ',') WITHIN GROUP (
ORDER BY
    SRC_COLUMN_POS) || ', nvl(lead(gueltig_ab) over(partition by partner order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) gueltig_bis ' || ', nvl(lead(insert_tst) over(partition by partner order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) wirksam_bis ' || ', nvl(lead(insert_tst) over(partition by partner order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) insert_tst_bis ' || ' FROM ' || t.DATABASE_CORE || '.' || lower(t.source_system || '_' || t.TABLE_NAME || ';') ddl
FROM
    v_table_column_all_distinct v
JOIN table_repo.table t ON
    (t.table_id = v.table_id)
WHERE
    use_core = TRUE
GROUP BY
    v.table_id,
    t.DATABASE_CORE,
    t.TABLE_NAME,
    t.DATABASE_VIEW,
    t.source_system )
SELECT
    table_id,
    ddl
FROM
    par_access_view_hist1;



SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW TABLE_REPO.V_TABLE_FILTER_CONDITION AS
SELECT
    t.DATABASE_CORE DATABASE_NAME,
     t.source_system || '_' || t.TABLE_NAME TABLE_NAME_DWH,
    'FILTER_CONDITION' PARAMETER,
    '{''' || LISTAGG(V.dwh_COLUMN_NAME,
    ''' ,''' ) WITHIN GROUP (
ORDER BY
    V.dwh_COLUMN_NAME ) || '''}' STRING_JSON
FROM
    TABLE_REPO.TABLE_COLUMN V
JOIN table_repo.table t ON
    (t.table_id = v.table_id)
WHERE
    V.MESSAGE_FILTER = true
GROUP BY
    t.TABLE_NAME,
    t.SID_COLUMN,
    t.DATABASE_CORE,
     t.source_system;



SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW table_repo.v_ddl_core AS  WITH v_table_column_all_distinct
AS (
    SELECT DISTINCT table_id,
                    table_name_dwh,
                    remarks,
                    src_column_pos,
                    ddl_line,
                    core_partition_pos,
                    use_core
    FROM TABLE_REPO.v_table_column_all),
                                                     COLUMN AS (
                                                         SELECT v.TABLE_ID,
                                                                v.TABLE_NAME_DWH,
                                                                ' (' || listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
                                                                                CASE
                                                                                    WHEN REMARKS IS NOT NULL
                                                                                        THEN ' COMMENT '''
                                                                                    ELSE ''
                                                                                    END || nvl(REMARKS,
                                                                                               '') ||
                                                                                CASE
                                                                                    WHEN REMARKS IS NOT NULL THEN ''''
                                                                                    ELSE ''
                                                                                    END,
                                                                                ', ') WITHIN GROUP (
                                                                                    ORDER BY
                                                                                        SRC_COLUMN_POS) lstD
                                                         FROM v_table_column_all_distinct v
                                                         WHERE v.CORE_PARTITION_POS IS NULL
                                                           AND v.USE_CORE = TRUE
                                                         GROUP BY TABLE_ID,
                                                                  TABLE_NAME_DWH),
                                                     PARTITION AS (
                                                         SELECT v.TABLE_ID,
                                                                ' PARTITIONED BY (' ||
                                                                listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
                                                                        CASE
                                                                            WHEN REMARKS IS NOT NULL THEN ' COMMENT '''
                                                                            ELSE ''
                                                                            END || nvl(REMARKS,
                                                                                       '') ||
                                                                        CASE
                                                                            WHEN REMARKS IS NOT NULL THEN ''''
                                                                            ELSE ''
                                                                            END,
                                                                        ', ') WITHIN GROUP (
                                                                            ORDER BY
                                                                                CORE_PARTITION_POS) || ') ' lst
                                                         FROM TABLE_REPO.v_table_column_all v
                                                         WHERE v.CORE_PARTITION_POS IS NOT NULL
                                                         GROUP BY TABLE_ID),
                                                     head AS (
                                                         SELECT t.table_id,
                                                                'CREATE TABLE IF NOT EXISTS  ' || t.DATABASE_CORE ||
                                                                '.' ||
                                                                LOWER(t.source_system || '_' || t.TABLE_NAME) head
                                                         FROM table_repo.table t
                                                         WHERE t.USAGE_FLAG = true and DATABASE_CORE is not null and DATABASE_CORE <> ''),
                                                     pk AS (
                                                         SELECT v.TABLE_ID,
                                                                ', primary key(' ||
                                                                listagg(CAST(v.COLUMN_NAME AS VARCHAR(30000)),
                                                                        ', ') WITHIN GROUP (
                                                                            ORDER BY
                                                                                CORE_SID_COL) ||
                                                                ') disable novalidate )' pk
                                                         FROM TABLE_REPO.v_table_column_all v
                                                         WHERE v.CORE_ID_COL = TRUE
                                                           AND v.CORE_PARTITION_POS IS NULL
                                                         GROUP BY TABLE_ID),
                                                     tail AS (
                                                         SELECT table_id,
                                                                ' STORED AS ' || t.FILE_TYPE_CORE ||
                                                                ' TBLPROPERTIES (''PARQUET.COMPRESS''=''SNAPPY'')' tail
                                                         FROM table_repo.table t),
                                                     COMMENT AS (
                                                         SELECT table_id,
                                                                CASE
                                                                    WHEN t.remarks IS NOT NULL
                                                                        AND LENGTH(t.remarkS) > 0
                                                                        THEN ' COMMENT ''' || t.REMARKs || ''''
                                                                    ELSE ''
                                                                    END COMMENT
                                                         FROM table_repo.table t)
                                                SELECT a.table_id,
                                                       b.table_name_dwh,
                                                       a.head,
                                                       b.lstD,
                                                       c.lst,
                                                       d.pk,
                                                       t.tail,
                                                       e.comment,
                                                       a.head || b.lstD || d.pk || e.comment || c.lst || t.tail || ';' ddl
                                                FROM head a
                                                         LEFT JOIN COLUMN b ON
                                                    (a.table_id = b.table_id)
                                                         LEFT JOIN PK d ON
                                                    (a.TABLE_ID = d.TABLE_ID)
                                                         LEFT JOIN PARTITION c ON
                                                    (c.TABLE_ID = a.table_id)
                                                         LEFT JOIN tail t ON
                                                    (t.table_id = a.table_id)
                                                         LEFT JOIN COMMENT e ON
                                                    (e.table_id = a.table_id);



SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW table_repo.v_ddl_data_mart AS  WITH v_table_column_all_distinct
AS (
    SELECT DISTINCT table_id,
                    table_name_dwh,
                    remarks,
                    src_column_pos,
                    ddl_line,
                    core_partition_pos,
                    use_core,
                    USE_DM
    FROM TABLE_REPO.v_table_column_all),
                                                     COLUMN AS (
                                                         SELECT v.TABLE_ID,
                                                                v.TABLE_NAME_DWH,
                                                                ' (' || listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
                                                                                CASE
                                                                                    WHEN REMARKS IS NOT NULL
                                                                                        THEN ' COMMENT '''
                                                                                    ELSE ''
                                                                                    END || nvl(REMARKS,
                                                                                               '') ||
                                                                                CASE
                                                                                    WHEN REMARKS IS NOT NULL THEN ''''
                                                                                    ELSE ''
                                                                                    END,
                                                                                ', ') WITHIN GROUP (
                                                                                    ORDER BY
                                                                                        SRC_COLUMN_POS) lstD
                                                         FROM v_table_column_all_distinct v
                                                         WHERE v.CORE_PARTITION_POS IS NULL
                                                           AND v.use_dm = TRUE
                                                         GROUP BY TABLE_ID,
                                                                  TABLE_NAME_DWH),
                                                     PARTITION AS (
                                                         SELECT v.TABLE_ID,
                                                                ' PARTITIONED BY (' ||
                                                                listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
                                                                        CASE
                                                                            WHEN REMARKS IS NOT NULL THEN ' COMMENT '''
                                                                            ELSE ''
                                                                            END || nvl(REMARKS,
                                                                                       '') ||
                                                                        CASE
                                                                            WHEN REMARKS IS NOT NULL THEN ''''
                                                                            ELSE ''
                                                                            END,
                                                                        ', ') WITHIN GROUP (
                                                                            ORDER BY
                                                                                CORE_PARTITION_POS) || ') ' lst
                                                         FROM TABLE_REPO.v_table_column_all v
                                                         WHERE v.CORE_PARTITION_POS IS NOT NULL
                                                         GROUP BY TABLE_ID),
                                                     head AS (
                                                         SELECT t.table_id,
                                                                'CREATE TABLE IF NOT EXISTS  ' || t.DATABASE_DATAMART ||
                                                                '.' ||
                                                                LOWER(t.source_system || '_' || t.TABLE_NAME) head
                                                         FROM table_repo.table t
                                                         WHERE t.USAGE_FLAG = true and DATABASE_DATAMART is not null and DATABASE_DATAMART <> ''),
                                                     pk AS (
                                                         SELECT v.TABLE_ID,
                                                                ', primary key(' ||
                                                                listagg(CAST(v.COLUMN_NAME AS VARCHAR(30000)),
                                                                        ', ') WITHIN GROUP (
                                                                            ORDER BY
                                                                                CORE_SID_COL) ||
                                                                ') disable novalidate )' pk
                                                         FROM TABLE_REPO.v_table_column_all v
                                                         WHERE v.CORE_ID_COL = TRUE
                                                           AND v.CORE_PARTITION_POS IS NULL
                                                         GROUP BY TABLE_ID),
                                                     tail AS (
                                                         SELECT table_id,
                                                                ' STORED AS ' || t.FILE_TYPE_CORE ||
                                                                ' TBLPROPERTIES (''PARQUET.COMPRESS''=''SNAPPY'')' tail
                                                         FROM table_repo.table t),
                                                     COMMENT AS (
                                                         SELECT table_id,
                                                                CASE
                                                                    WHEN t.remarks IS NOT NULL
                                                                        AND LENGTH(t.remarkS) > 0
                                                                        THEN ' COMMENT ''' || t.REMARKs || ''''
                                                                    ELSE ''
                                                                    END COMMENT
                                                         FROM table_repo.table t)
                                                SELECT a.table_id,
                                                       b.table_name_dwh,
                                                       a.head,
                                                       b.lstD,
                                                       c.lst,
                                                       d.pk,
                                                       t.tail,
                                                       e.comment,
                                                       a.head || b.lstD || nvl(d.pk,'') || e.comment || nvl(c.lst,'') || t.tail || ';' ddl
                                                FROM head a
                                                         LEFT JOIN COLUMN b ON
                                                    (a.table_id = b.table_id)
                                                         LEFT JOIN PK d ON
                                                    (a.TABLE_ID = d.TABLE_ID)
                                                         LEFT JOIN PARTITION c ON
                                                    (c.TABLE_ID = a.table_id)
                                                         LEFT JOIN tail t ON
                                                    (t.table_id = a.table_id)
                                                         LEFT JOIN COMMENT e ON
                                                    (e.table_id = a.table_id);



SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.v_docu_dm as
with s1 as (
    select t.DATABASE_DATAMART DATABASE,
           lower(t.TABLE_NAME) TABLE,
           c.COLUMN,
           ROW_NUMBER() OVER (partition by table_name order by SRC_COLUMN_POS  ) COLUMN_POS,
           c.DWH_TYPE TYPE,
           c.DWH_LENGTH LENGTH,
           c.DWH_SCALE SCALE,
           c.REMARKS REMARKS
    from TABLE_REPO.TABLE t
             join TABLE_REPO.V_TABLE_COLUMN_ALL c on (c.table_id = t.table_id)
    where t.USAGE_FLAG = true
      and c.USE_DM = true

)
select *
from s1;



SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.V_DDL_DATA_MART_ACCESS_VIEW as
with v_table_column_all_distinct as (select distinct table_id, table_name_dwh, column_name,remarks, src_column_pos, ddl_line, core_partition_pos, use_core, USE_DM from TABLE_REPO.v_table_column_all),
     access_view as (select v.table_id,
                            'CREATE OR REPLACE VIEW ' || t.DATABASE_DM_VIEW || '.v_' || lower(t.source_system || '_' || t.TABLE_NAME) ||
                            ' AS SELECT ' ||
                            listagg(lower(nvl(v.COLUMN_NAME, v.COLUMN_NAME)), ',')
                                    within group ( order by SRC_COLUMN_POS) || ' FROM ' || t.DATABASE_DATAMART || '.' ||
                            lower(t.source_system || '_' || t.TABLE_NAME || ';') ddl
                     from v_table_column_all_distinct v
                              join table_repo.table t on (t.table_id = v.table_id)
                     where use_dm = true
                     group by v.table_id, t.DATABASE_DATAMART, t.TABLE_NAME,t.DATABASE_DM_VIEW, t.SOURCE_SYSTEM
)
select *
from access_view a;



SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE     VIEW TABLE_REPO.v_table_column_all AS
WITH src_row AS (
    SELECT a.TABLE_ID,
           LOWER(nvl(a.DWH_COLUMN_NAME, SRC_COLUMN_NAME)) COLUMN_NAME,
           a.SRC_COLUMN_POS,
           a.USE_INTERFACE,
           a.USE_RAW_VAULT,
           a.USE_CORE,
           a.USE_DM,
           a.SRC_TYPE,
           a.SRC_LENGTH,
           a.SRC_SCALE,
           a.SRC_NOT_NULL,
           a.CORE_SID_COL,
           a.CORE_ID_COL,
           a.DWH_COLUMN_NAME,
           a.CORE_PARTITION_POS,
           a.REMARKS
    FROM TABLE_REPO.TABLE_COLUMN a
             JOIN TABLE_REPO.TABLE b ON
        (a.TABLE_ID = b.TABLE_ID)
    WHERE b.USAGE_FLAG = TRUE),
                                               share_col AS (
                                                   SELECT a.TABLE_ID,
                                                          LOWER(b.COLUMN_NAME) COLUMN_NAME,
                                                          b.COLUMN_POS,
                                                          b.USE_INTERFACE,
                                                          b.USE_RAW_VAULT,
                                                          b.USE_CORE,
                                                          b.USE_DM,
                                                          b.TYPE               SRC_TYPE,
                                                          b.LENGTH             SRC_LENGTH,
                                                          b.SCALE              SRC_SCALE,
                                                          NULL                 SRC_NOT_NULL,
                                                          b.CORE_SID_COL,
                                                          b.CORE_ID_COL,
                                                          NULL                 DWH_COLUMN_NAME,
                                                          b.CORE_PARTITION_POS,
                                                          b.REMARKS
                                                   FROM TABLE_REPO.TABLE_SHARED_COLUMN b
                                                            JOIN TABLE_REPO.TABLE a ON
                                                       (a.TABLE_GROUP_ID = b.TABLE_GROUP_ID)
                                                   WHERE a.USAGE_FLAG = TRUE),
                                               sid_col AS (
                                                   SELECT a.table_id,
                                                          LOWER(a.SID_COLUMN) COLUMN_NAME,
                                                          0                   SRC_COLUMN_POS,
                                                          FALSE               USE_INTERFACE,
                                                          FALSE               USE_RAW_VAULT,
                                                         case when a.DATABASE_CORE is not null or a.DATABASE_CORE <> '' then true else false end                USE_CORE,
                                                         case when a.DATABASE_DATAMART is not null or a.DATABASE_DATAMART <> '' then true else false end                USE_DM,
                                                          'STRING'            SRC_TYPE,
                                                          NULL                SRC_LENGTH,
                                                          NULL                SRC_SCALE,
                                                          NULL                SRC_NOT_NULL,
                                                          FALSE               CORE_SID_COL,
                                                          FALSE               CORE_ID_COL,
                                                          NULL                DWH_COLUMN_NAME,
                                                          NULL                CORE_PARTITION_POS,
                                                          'Surrgate Key'      REMARKS
                                                   FROM TABLE_REPO.TABLE a
                                                   WHERE a.USAGE_FLAG = TRUE and a.SID_COLUMN is not null and a.SID_COLUMN <>''),
                                               id_col AS (
                                                   SELECT a.table_id,
                                                          LOWER(a.ID_COLUMN)      COLUMN_NAME,
                                                          1                       SRC_COLUMN_POS,
                                                          FALSE                   USE_INTERFACE,
                                                          FALSE                   USE_RAW_VAULT,
                                                          case when a.DATABASE_CORE is not null or a.DATABASE_CORE <> '' then true else false end                         USE_CORE,
                                                          case when a.DATABASE_DATAMART is not null or a.DATABASE_DATAMART <> '' then true else false end              USE_DM,
                                                          'STRING'                SRC_TYPE,
                                                          NULL                    SRC_LENGTH,
                                                          NULL                    SRC_SCALE,
                                                          NULL                    SRC_NOT_NULL,
                                                          FALSE                   CORE_SID_COL,
                                                          FALSE                   CORE_ID_COL,
                                                          NULL                    DWH_COLUMN_NAME,
                                                          NULL                    CORE_PARTITION_POS,
                                                          'Foreign Key' REMARKS
                                                   FROM TABLE_REPO.TABLE a
                                                   WHERE a.USAGE_FLAG = TRUE and a.ID_COLUMN is not null and a.ID_COLUMN <> ''),
                                               fk_id_col AS (
                                                   SELECT a.table_id,
                                                          LOWER(c.ID_COLUMN) COLUMN_NAME,
                                                          20                 SRC_COLUMN_POS,
                                                          FALSE              USE_INTERFACE,
                                                          FALSE              USE_RAW_VAULT,
                                                         case when a.DATABASE_CORE is not null or a.DATABASE_CORE <> '' then true else false end                     USE_CORE,
                                                        case when a.DATABASE_DATAMART is not null or a.DATABASE_DATAMART <> '' then true else false end             USE_DM,
                                                          'STRING'           SRC_TYPE,
                                                          NULL               SRC_LENGTH,
                                                          NULL               SRC_SCALE,
                                                          NULL               SRC_NOT_NULL,
                                                          FALSE              CORE_SID_COL,
                                                          FALSE              CORE_ID_COL,
                                                          NULL               DWH_COLUMN_NAME,
                                                          NULL               CORE_PARTITION_POS,
                                                          'Foreign Key'      REMARKS
                                                   FROM TABLE_REPO.TABLE a
                                                            JOIN TABLE_REPO.TABLE_REPO_FOREIGN_KEY b ON
                                                       (a.TABLE_ID = b.TABLE_ID)
                                                            JOIN TABLE_REPO.TABLE c ON
                                                       (b.TABLE_REF_ID = c.TABLE_ID)
                                                   WHERE a.USAGE_FLAG = TRUE AND b.FKEY_ID=true),
                                               fk_sid_col AS (
                                                   SELECT a.table_id,
                                                          LOWER(c.SID_COLUMN) COLUMN_NAME,
                                                          10                  SRC_COLUMN_POS,
                                                          FALSE               USE_INTERFACE,
                                                          FALSE               USE_RAW_VAULT,
                                                        case when a.DATABASE_CORE is not null or a.DATABASE_CORE <> '' then true else false end                       USE_CORE,
                                                          case when a.DATABASE_DATAMART is not null or a.DATABASE_DATAMART <> '' then true else false end               USE_DM,
                                                          'STRING'            SRC_TYPE,
                                                          NULL                SRC_LENGTH,
                                                          NULL                SRC_SCALE,
                                                          NULL                SRC_NOT_NULL,
                                                          FALSE               CORE_SID_COL,
                                                          FALSE               CORE_ID_COL,
                                                          NULL                DWH_COLUMN_NAME,
                                                          NULL                CORE_PARTITION_POS,
                                                          'Foreign Key'       REMARKS
                                                   FROM TABLE_REPO.TABLE a
                                                            JOIN TABLE_REPO.TABLE_REPO_FOREIGN_KEY b ON
                                                       (a.TABLE_ID = b.TABLE_ID)
                                                            JOIN TABLE_REPO.TABLE c ON
                                                       (b.TABLE_REF_ID = c.TABLE_ID)
                                                   WHERE a.USAGE_FLAG = TRUE AND b.FKEY_SID=true),
                                               join_columns as (
                                                   select t.table_id,
                                                          c.table_id                                joined_table_id,
                                                          j.join_id,
                                                          c.src_column_name,
                                                          nvl(c.column_name_new, c.src_column_name) column_name_new,
                                                          COL_POS_START,
                                                          col_pos_offset
                                                   from join_2_table t
                                                            join join j on (t.join_id = j.join_id)
                                                            join join_column c on (c.join_id = t.JOIN_ID)
                                               ),
                                               all_columns AS (
                                                   SELECT *
                                                   FROM fk_sid_col
                                                   UNION ALL
                                                   SELECT *
                                                   FROM fk_id_col
                                                   UNION ALL
                                                   SELECT *
                                                   FROM id_col
                                                   UNION ALL
                                                   SELECT *
                                                   FROM sid_col
                                                   UNION ALL
                                                   SELECT *
                                                   FROM src_row
                                                   UNION ALL
                                                   SELECT *
                                                   FROM share_col
                                               ),
                                               add_dwh_type AS (
                                                   SELECT a.*,
                                                          LOWER(CASE
                                                                    WHEN src_type = 'VARCHAR' THEN 'STRING'
                                                                    WHEN src_type = 'DATE' THEN 'TIMESTAMP'
                                                                    WHEN src_type = 'CHARACTER' THEN 'STRING'
                                                                    WHEN src_type = 'CHAR' THEN 'STRING'
                                                                    WHEN src_type = 'CLNT' THEN 'STRING'
                                                                    WHEN src_type = 'NUMC' THEN 'STRING'
                                                                    WHEN src_type = 'DATS' THEN 'STRING'
                                                                    WHEN src_type = 'TIMS' THEN 'STRING'
                                                                    WHEN src_type = 'RAW' THEN 'STRING'
                                                                    WHEN src_type = 'LANG' THEN 'STRING'
                                                                    WHEN src_type = 'DEC' THEN 'DECIMAL'
                                                                    ELSE src_type END) dwh_type,
                                                          CASE
                                                              WHEN src_type = 'DECIMAL' THEN SRC_LENGTH
                                                              ELSE NULL
                                                              END                      DWH_LENGTH,
                                                          CASE
                                                              WHEN src_type = 'DECIMAL' THEN SRC_scale
                                                              ELSE NULL
                                                              END                      DWH_SCALE
                                                   FROM all_columns a),
                                               create_col_stmt AS (
                                                   SELECT a.*,
                                                          LOWER(nvl(a.DWH_COLUMN_NAME, a.COLUMN_NAME)) || '  ' ||
                                                          UPPER(a.DWH_TYPE) ||
                                                          CASE
                                                              WHEN a.DWH_LENGTH IS NOT NULL
                                                                  THEN '(' || TRIM(a.DWH_LENGTH) || ',' || TRIM(a.DWH_SCALE) || ')'
                                                              ELSE ''
                                                              END                                  ddl_line,
                                                          LOWER(nvl(DWH_COLUMN_NAME, COLUMN_NAME)) COLUMN
                                                   FROM add_dwh_type a),
                                               new_col as (
                                                   select j.table_id,
                                                          lower(j.column_name_new)                           column_name,
                                                          col_pos_start + col_pos_offset                     src_column_pos,
                                                          false                                              use_interface,
                                                          false                                              use_raw_vault,
                                                          use_core,
                                                          use_dm,
                                                          src_type,
                                                          src_length,
                                                          src_scale,
                                                          src_not_null,
                                                          false                                              core_sid_col,
                                                          false                                              core_id_col,
                                                          dwh_column_name,
                                                          null                                               core_partition_pos,
                                                          remarks,
                                                          dwh_type,
                                                          dwh_length,
                                                          dwh_scale,
                                                          lower(j.column_name_new) || ' ' || upper(dwh_type) ddl_line,
                                                          column
                                                   from create_col_stmt c
                                                            join join_columns j
                                                                 on (lower(j.joined_table_id) = lower(c.table_id) and
                                                                     lower(j.src_column_name) = lower(c.column_name))
                                               ),
                                               final_columns AS (
                                                   SELECT *
                                                   FROM create_col_stmt
                                                   UNION ALL
                                                   SELECT *
                                                   FROM new_col
                                               )
                                          SELECT distinct t.SOURCE_SYSTEM || '_' || t.TABLE_NAME TABLE_name_dwh,
                                                 c.*, t.KEY_TABLE, t.DATABASE_CORE, t.DATABASE_VIEW
                                          FROM final_columns c
                                                   JOIN table_repo.TABLE t ON
                                              (c.TABLE_id = t.table_id);


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.v_docu_raw_vault as
with s1 as (
    select t.DATABASE_RAW_VAULT DATABASE,
           lower(t.TABLE_NAME) TABLE,
           c.COLUMN,
           ROW_NUMBER() OVER (partition by table_name order by SRC_COLUMN_POS  ) COLUMN_POS,
           c.DWH_TYPE TYPE,
           c.DWH_LENGTH LENGTH,
           c.DWH_SCALE SCALE,
           c.REMARKS REMARKS
    from TABLE_REPO.TABLE t
             join TABLE_REPO.V_TABLE_COLUMN_ALL c on (c.table_id = t.table_id)
    where t.USAGE_FLAG = true
      and c.USE_RAW_VAULT = true

)
select *
from s1;


COMMENT ON TABLE "TABLE_REPO"."V_DOCU_RAW_VAULT" IS 'Contains definition of all raw_zone tables. All these defintions are extracted from db2 catalog';

SET CURRENT SCHEMA = "JOB_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.v_docu_interface as
with s1 as (
    select t.TABLE_SCHEMA SCHEMA_QUELLSYSTEM,
           t.TABLE_NAME TABELLENNAME,
           COLUMN_NAME SPALTENNAME,
           c.SRC_COLUMN_POS SPALTENPOSITION,
           c.SRC_TYPE DATENTYP,
           c.SRC_LENGTH LAENGE,
           c.SRC_SCALE SCALE,
           c.SRC_NOT_NULL NULLABLE,
           c.REMARKS BESCHREIBUNG
    from TABLE_REPO.TABLE t
             join TABLE_REPO.V_TABLE_COLUMN_ALL c on (c.table_id = t.table_id)
    where t.USAGE_FLAG = true
      and c.USE_INTERFACE = true
)
select *
from s1;


COMMENT ON TABLE "TABLE_REPO"."V_DOCU_INTERFACE" IS 'Provides docu about data provided by source system to be used in confluence. 
		Defines all fields of source system tables used in thw dwh. Doesnot have any technical fields';

SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW table_repo.v_table_foreign_key_parameter AS
    WITH sid_json AS (
SELECT
    c.source_system || '_' || c.TABLE_NAME table_name_dwh,
    c.DATABASE_CORE,
    d.SID_COLUMN,
    a.table_id,
    a.TABLE_REF_ID,
    'FOREIGN_KEY' PARAMETER,
    '"' || LOWER(d.SID_COLUMN) || '":["' || listagg(e.column_name,
    '","') WITHIN GROUP (
ORDER BY
    e.COLUMN_NAME) || '"]' STR_JSON
FROM
    table_repo.TABLE_REPO_FOREIGN_KEY a
left JOIN table_repo."TABLE" c ON
    (c.TABLE_ID = a.TABLE_ID)
left JOIN table_repo.TABLE d ON
    (d.TABLE_ID = a.TABLE_REF_ID)
left  JOIN  table_repo.V_TABLE_COLUMN_ALL e on
    (a.table_ref_id = e.table_id)
WHERE
    e.core_sid_col IS TRUE
    and a.fkey_sid = true
GROUP BY
    c.source_system || '_' || c.TABLE_NAME,
    c.DATABASE_CORE,
    d.SID_COLUMN,
          a.table_id,
    a.TABLE_REF_ID ),
id_json AS (
SELECT
    c.source_system || '_' || c.TABLE_NAME table_name_dwh,
    c.DATABASE_CORE,
    d.ID_COLUMN,
    a.table_id,
    a.TABLE_REF_ID,
    'FOREIGN_KEY' PARAMETER,
    '"' || LOWER(d.ID_COLUMN) || '":["' || listagg(e.column_name,
    '","') WITHIN GROUP (
ORDER BY
    e.COLUMN_NAME) || '"]' STR_JSON
FROM
    table_repo.TABLE_REPO_FOREIGN_KEY a
left JOIN table_repo."TABLE" c ON
    (c.TABLE_ID = a.TABLE_ID)
left JOIN table_repo.TABLE d ON
    (d.TABLE_ID = a.TABLE_REF_ID)
left  JOIN  table_repo.V_TABLE_COLUMN_ALL e on
    (a.table_ref_id = e.table_id)
WHERE
    e.core_id_col IS TRUE
    and a.fkey_id = true
GROUP BY
    c.source_system || '_' || c.TABLE_NAME,
    c.DATABASE_CORE,
    d.ID_COLUMN,
          a.table_id,
    a.TABLE_REF_ID),
agg_all AS (
SELECT
    table_name_dwh,
    database_core,
    str_json
FROM
    id_json
UNION ALL
SELECT
    table_name_dwh,
    database_core,
    str_json
FROM
    sid_json )
SELECT
    table_name_dwh,
    database_core,
    'FOREIGN_KEY' PARAMETER,
    listagg(CAST(str_json AS VARCHAR(30000)),
    ',') WITHIN GROUP (
ORDER BY
    1) str_json
FROM
    agg_all a
GROUP BY
    table_name_dwh,
    database_core;


COMMENT ON TABLE "TABLE_REPO"."V_TABLE_FOREIGN_KEY_PARAMETER" IS 'Returns json definition of foregin key relationships for defining paraemters in job_repo. 
		For example, key_type_id and key_type_sid in v_job_parameter takes the values set in v_table_FOREIGN_KEY_PARAMETER';

SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view v_ddl_core_key as
with tab_col_all as (
    select key_table,
           column_name,
           ddl_line,
           DATABASE_CORE,
           CORE_PARTITION_POS,
           FIRST_VALUE(src_column_pos) OVER (PARTITION BY KEY_TABLE ORDER BY SRC_COLUMN_POS) col_pos
    from V_TABLE_COLUMN_ALL
    where core_id_col is true
      and KEY_TABLE is not null
      and key_table not like ''
    union
    select key_table, column_name, ddl_line, DATABASE_CORE, core_partition_pos, src_column_pos col_pos
    from V_TABLE_COLUMN_ALL o
    where key_table is not null
      and key_table not like ''
      and exists(select * from TABLE_SHARED_COLUMN i where lower(i.column_name) = lower(o.column_name))),
     agg_fields as (
         select key_table,
                DATABASE_CORE,
                ' (' || listagg(ddl_line, ', ') within group (order by col_pos ) || ') ' fields
         from tab_col_all
         where CORE_PARTITION_POS is null
         group by key_table, database_core
     ),
     agg_part_fields as (
         select key_table,
                DATABASE_CORE,
                ' (' || listagg(ddl_line, ', ') within group (order by col_pos ) || ') ' agg_fields
         from tab_col_all
         where CORE_PARTITION_POS is not null
         group by key_table, database_core
     ),
     create_stmt as (
         select af.key_table,
                af.database_core,
                af.fields,
                apf.agg_fields,
                'CREATE TABLE ' || af.database_core || '.' || af.key_table || af.fields || ' PARTITIONED BY ' ||
                agg_fields as ddl_stmt
         from agg_fields af
                  join agg_part_fields apf on (af.KEY_TABLE = apf.key_table)
     )

select *
from create_stmt;


COMMENT ON TABLE "TABLE_REPO"."V_DDL_CORE_KEY" IS 'Contains ddl stmts for creating key tables';

SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view v_cognos_tabellen_spalten as
select lower(table_id)                                    table_id,
       lower(table_name_dwh)                              tabellen_name_dwh,
       lower(column_name)                             Feld_name,
       dwh_type                                           datentyp,
       dwh_length                                         länge,
       DWH_SCALE                                          scale,
       case when core_sid_col = true then 'X' else '' end Primary_Key_Feld,
       case when core_id_col = true then 'X' else '' end  ID_Feld,
       remarks                                            beschreibung
from v_table_column_all;


SET CURRENT SCHEMA = "DB2INST1";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE 
VIEW table_repo.v_table_join AS WITH new_columns_list AS(
SELECT
    jc.join_id,
    jc.table_id,
    j.src_schema,
    '["' || jc.src_column_name || '","' || nvl(jc.column_name_new,
    jc.src_column_name) || '"]' new_cols
FROM
    table_repo.JOIN_COLUMN jc
JOIN table_repo."JOIN" j ON
    (jc.join_id = j.join_id)
JOIN table_repo."TABLE" t ON
    (jc.table_id = t.table_id)
GROUP BY
    jc.join_id,
    jc.table_id,
    jc.SRC_COLUMN_NAME,
    jc.COLUMN_NAME_NEW,
    j.src_schema),
new_columns AS (
SELECT
    join_id,
    table_id,
    '"join_schema":"' || src_schema || '"' join_schema,
    '"new_columns":[' || listagg(new_cols,
    ',') WITHIN GROUP(
ORDER BY
    join_id) || ']' new_columns_string
FROM
    new_columns_list
GROUP BY
    join_id,
    table_id,
    src_schema),
columns_name AS (
SELECT
	j2.table_id,
	v.table_name_dwh
FROM 
	table_repo.join_2_table j2
JOIN table_repo.v_table_column_all v ON
	(j2.table_id = v.table_id)
GROUP BY
	j2.table_id,
	v.table_name_dwh
),
table_name AS (
SELECT
    jc.join_id,
    '"joined_table":"' || v.table_name_dwh || '"' name_dwh
FROM
    table_repo.join_condition jc
JOIN table_repo.v_table_column_all v ON
    (v.table_id = jc.table_id)
GROUP BY
    jc.join_id,
    v.table_name_dwh ),
join_condition_2 AS (
SELECT
    join_id,
    table_id,
    '"join_columns":["' || listagg(SRC_COLUMN_NAME,
    '","') WITHIN GROUP (
ORDER BY
    join_id) || '"]' join_condition_string
FROM
    table_repo.JOIN_CONDITION
GROUP BY
    join_id,
    table_id ),
agg_prepare AS (
SELECT
    jc.join_id,
    c.table_name_dwh,
    jc.join_condition_string,
    nc.new_columns_string,
    nc.join_schema,
    tn.name_dwh,
    '"join_type":"' || j2.join_type || '"' join_type
FROM
    join_condition_2 jc
JOIN table_repo.join_2_table j2 ON
    (jc.join_id = j2.join_id)
JOIN new_columns nc ON
    (jc.join_id = nc.join_id)
JOIN table_repo."JOIN" j ON
    (jc.join_id = j.join_id)
JOIN table_name tn ON
    (jc.join_id = tn.join_id)
JOIN columns_name c ON
	(j2.table_id = c.table_id)),
str_cte AS (
SELECT
    table_name_dwh,
    '{' || join_schema || ',' || name_dwh || ',' || join_condition_string || ',' || new_columns_string || ',' || join_type || '}' join_string
FROM
    agg_prepare )
SELECT
    table_name_dwh,
    'core' database_name,
    'JOIN_TABLE' PARAMETER,
    listagg(CAST(LOWER(join_string) AS VARCHAR(3000)),
    ',') WITHIN GROUP (
ORDER BY
    table_name_dwh) string_json
FROM
    str_cte
GROUP BY
    table_name_dwh;


COMMENT ON TABLE "TABLE_REPO"."V_TABLE_JOIN" IS 'Same as V_TABLE_FOREIGN_KEY_PARAMETER but with join condition for job_repo ';

SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW TABLE_REPO.v_table_key_parameter AS WITH fields_sid AS (
SELECT
    t.DATABASE_CORE DATABASE,
    t.source_system || '_' || t.TABLE_NAME TABLE,
    'SID' keytype,
    'KEY_TYPE_SID' PARAMETER,
    '"sid":{"field_name":"' || LOWER(t.SID_COLUMN) || '","fields":["' || LISTAGG(V.COLUMN_NAME,
    '","') WITHIN GROUP (
ORDER BY
    V.COLUMN_NAME ) || '"]}' KEY
FROM
    TABLE_REPO.V_TABLE_COLUMN_ALL V
JOIN table_repo.table t ON
    (t.table_id = v.table_id)
WHERE
    V.CORE_SID_COL = TRUE
GROUP BY
     t.source_system || '_' || t.TABLE_NAME,
    t.SID_COLUMN,
    t.DATABASE_CORE),
fields_id AS (
SELECT
    t.DATABASE_CORE DATABASE,
     t.source_system || '_' || t.TABLE_NAME TABLE,
    'ID' keytype,
    'KEY_TYPE_ID' PARAMETER,
    '"id":{"field_name":"' || LOWER(t.ID_COLUMN) || '","fields":["' || LISTAGG(V.COLUMN_NAME,
    '","') WITHIN GROUP (
ORDER BY
    V.COLUMN_NAME ) || '"]}' KEY
FROM
    TABLE_REPO.V_TABLE_COLUMN_ALL V
JOIN table_repo.table t ON
    (t.table_id = v.table_id)
WHERE
    V.CORE_ID_COL = TRUE
GROUP BY
    t.source_system || '_' || t.TABLE_NAME,
    t.ID_COLUMN,
    t.DATABASE_CORE)
SELECT
    *
FROM
    fields_id
UNION ALL
SELECT
    *
FROM
    fields_sid;


COMMENT ON TABLE "TABLE_REPO"."V_TABLE_KEY_PARAMETER" IS 'keys definition for job_repo';

SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.V_DDL_CORE_ACCESS_VIEW as
with v_table_column_all_distinct as (select distinct table_id, table_name_dwh, column_name,remarks, src_column_pos, ddl_line, core_partition_pos, use_core from TABLE_REPO.v_table_column_all),
     access_view as (select v.table_id,
                            'CREATE OR REPLACE VIEW ' || t.DATABASE_VIEW || '.v_' || lower(t.source_system || '_' || t.TABLE_NAME) ||
                            ' AS SELECT ' ||
                            listagg(lower(nvl(v.COLUMN_NAME, v.COLUMN_NAME)), ',')
                                    within group ( order by SRC_COLUMN_POS) || ' FROM ' || t.DATABASE_CORE || '.' ||
                            lower(t.source_system || '_' || t.TABLE_NAME || ';') ddl
                     from v_table_column_all_distinct v
                              join table_repo.table t on (t.table_id = v.table_id)
                     where use_core = true
                     group by v.table_id, t.DATABASE_CORE, t.TABLE_NAME,t.DATABASE_VIEW, t.SOURCE_SYSTEM
)
select *
from access_view a;


COMMENT ON TABLE "TABLE_REPO"."V_DDL_CORE_ACCESS_VIEW" IS 'provides access views FOR data mart tables. This VIEW takes all columns from core tables. Access views are used by end users for accesing data mart tables. This is due to security reasons';

SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.V_DDL_CORE_HIST as
   with  v_table_column_all_distinct as (select distinct table_id, column_name,table_name_dwh, remarks, src_column_pos, ddl_line, core_partition_pos, use_core from TABLE_REPO.v_table_column_all),
pas_access_view_hist1 as (select v.table_id,
                            'CREATE OR REPLACE VIEW ' || t.DATABASE_VIEW || '.v_' ||
                            lower(t.source_system || '_' || t.TABLE_NAME) ||
                            '_hist AS SELECT ' ||
                            listagg(lower(nvl(v.COLUMN_NAME, v.COLUMN_NAME)), ',')
                                    within group ( order by SRC_COLUMN_POS)
                                 || ', nvl(lead(gueltig_ab) over(partition by lvid order by bearbid),cast (''2099-12-31 23:59:59'' as timestamp)) gueltig_bis '
                                || ', nvl(lead(wirksam_ab) over(partition by lvid order by bearbid),cast (''2099-12-31 23:59:59'' as timestamp)) wirksam_bis '
                                || ', nvl(lead(insert_tst) over(partition by lvid order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) insert_tst_bis '
                                || ' FROM ' || t.DATABASE_CORE || '.' ||
                            lower(t.source_system || '_' || t.TABLE_NAME || ';') ddl
                     from v_table_column_all_distinct v
                              join table_repo.table t on (t.table_id = v.table_id)
                     where use_core = true
                       and exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'gueltig_ab')
                       and exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'wirksam_ab')
                        and exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'bearbid')
                     group by v.table_id, t.DATABASE_CORE, t.TABLE_NAME, t.DATABASE_VIEW, t.source_system
),
     pas_access_view_hist2 as (select v.table_id,
                            'CREATE OR REPLACE VIEW ' || t.DATABASE_VIEW || '.v_' ||
                            lower(t.source_system || '_' || t.TABLE_NAME) ||
                            '_hist AS SELECT ' ||
                            listagg(lower(nvl(v.COLUMN_NAME, v.COLUMN_NAME)), ',')
                                    within group ( order by SRC_COLUMN_POS)
                                || ', insert_tst wirksam_ab '
                                || ', insert_tst gueltig_ab '
                               || ', nvl(lead(insert_tst) over(partition by lvid order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) gueltig_bis '
                                || ', nvl(lead(insert_tst) over(partition by lvid order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) wirksam_bis '
                                || ', nvl(lead(insert_tst) over(partition by lvid order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) insert_tst_bis '
                                || ' FROM ' || t.DATABASE_CORE || '.' ||
                            lower(t.source_system || '_' || t.TABLE_NAME || ';') ddl
                     from v_table_column_all_distinct v
                              join table_repo.table t on (t.table_id = v.table_id)
                     where use_core = true
                       and not exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'gueltig_ab')
                       and not exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'wirksam_ab')
                        and not exists(select *
                                  from table_repo.v_table_column_all i
                                  where i.table_id = t.table_id and i.column_name like 'bearbid')
                     group by v.table_id, t.DATABASE_CORE, t.TABLE_NAME, t.DATABASE_VIEW, t.source_system
)
select table_id, ddl
from pas_access_view_hist1
union all select table_id, ddl
from pas_access_view_hist2;


COMMENT ON TABLE "TABLE_REPO"."V_DDL_CORE_HIST" IS 'Adds temporara atteibutes to access views For ex: valid_from / valid_to columns';

SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW table_repo.v_ddl_raw_vault AS WITH COLUMN AS (
SELECT
    v.TABLE_ID,
    v.TABLE_NAME_DWH,
    ' (' || listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
    CASE
        WHEN REMARKS IS NOT NULL THEN ' COMMENT '''
        ELSE ''
    END || nvl(REMARKS,
    '') ||
    CASE
        WHEN REMARKS IS NOT NULL THEN ''''
        ELSE ''
    END,
    ', ') WITHIN GROUP (
ORDER BY
    SRC_COLUMN_POS) || ') ' lstD
FROM
    TABLE_REPO.v_table_column_all v
WHERE
    v.CORE_PARTITION_POS IS NULL
    AND v.USE_RAW_VAULT = TRUE
GROUP BY
    TABLE_ID,
    TABLE_NAME_DWH),
PARTITION AS (
SELECT
    v.TABLE_ID,
    ' PARTITIONED BY (' || listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
    CASE
        WHEN REMARKS IS NOT NULL THEN ' COMMENT '''
        ELSE ''
    END || nvl(REMARKS,
    '') ||
    CASE
        WHEN REMARKS IS NOT NULL THEN ''''
        ELSE ''
    END,
    ', ') WITHIN GROUP (
ORDER BY
    CORE_PARTITION_POS) || ') ' lst
FROM
    TABLE_REPO.v_table_column_all v
WHERE
    v.CORE_PARTITION_POS IS NOT NULL
GROUP BY
    TABLE_ID ),
head AS (
SELECT
    t.table_id,
    'CREATE TABLE IF NOT EXISTS  ' || t.DATABASE_RAW_VAULT || '.' || LOWER(t.source_system || '_' || t.TABLE_NAME) head
FROM
    table_repo.table t ),
tail AS (
SELECT
    table_id,
    ' STORED AS ' || t.FILE_TYPE_RAW_VAULT || ' TBLPROPERTIES (''PARQUET.COMPRESS''=''SNAPPY'')' tail
FROM
    table_repo.table t),
COMMENT AS (
SELECT
    table_id,
    CASE
        WHEN t.remarks IS NOT NULL
        AND LENGTH(t.remarkS) > 0 THEN ' COMMENT ''' || t.REMARKs || ''''
        ELSE ''
    END COMMENT
FROM
    table_repo.table t )
SELECT
    a.table_id,
    b.table_name_dwh,
    a.head,
    b.lstD,
    c.lst,
    t.tail,
    e.comment,
    a.head || b.lstD || e.comment || c.lst || t.tail || ';' ddl
FROM
    head a
LEFT JOIN COLUMN b ON
    (a.table_id = b.table_id)
LEFT JOIN PARTITION c ON
    (c.TABLE_ID = a.table_id)
LEFT JOIN tail t ON
    (t.table_id = a.table_id)
LEFT JOIN COMMENT e ON
    (e.table_id = a.table_id);


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW TABLE_REPO.V_DDL_CORE_HIST_SAP AS WITH v_table_column_all_distinct
AS (
SELECT
    DISTINCT table_id,
    column_name,
    table_name_dwh,
    remarks,
    src_column_pos,
    ddl_line,
    core_partition_pos,
    use_core
FROM
    TABLE_REPO.v_table_column_all
WHERE
	TABLE_NAME_DWH LIKE 'PAR%'),
par_access_view_hist1 AS (
SELECT
    v.table_id,
    'CREATE OR REPLACE VIEW ' || t.DATABASE_VIEW || '.v_' || lower(t.source_system || '_' || t.TABLE_NAME) || '_hist AS SELECT ' || listagg(lower(nvl(v.COLUMN_NAME, v.COLUMN_NAME)),
    ',') WITHIN GROUP (
ORDER BY
    SRC_COLUMN_POS) || ', nvl(lead(gueltig_ab) over(partition by partner order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) gueltig_bis ' || ', nvl(lead(insert_tst) over(partition by partner order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) wirksam_bis ' || ', nvl(lead(insert_tst) over(partition by partner order by insert_tst),cast (''2099-12-31 23:59:59'' as timestamp)) insert_tst_bis ' || ' FROM ' || t.DATABASE_CORE || '.' || lower(t.source_system || '_' || t.TABLE_NAME || ';') ddl
FROM
    v_table_column_all_distinct v
JOIN table_repo.table t ON
    (t.table_id = v.table_id)
WHERE
    use_core = TRUE
GROUP BY
    v.table_id,
    t.DATABASE_CORE,
    t.TABLE_NAME,
    t.DATABASE_VIEW,
    t.source_system )
SELECT
    table_id,
    ddl
FROM
    par_access_view_hist1;


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW table_repo.v_ddl_core AS  WITH v_table_column_all_distinct
AS (
    SELECT DISTINCT table_id,
                    table_name_dwh,
                    remarks,
                    src_column_pos,
                    ddl_line,
                    core_partition_pos,
                    use_core
    FROM TABLE_REPO.v_table_column_all),
                                                     COLUMN AS (
                                                         SELECT v.TABLE_ID,
                                                                v.TABLE_NAME_DWH,
                                                                ' (' || listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
                                                                                CASE
                                                                                    WHEN REMARKS IS NOT NULL
                                                                                        THEN ' COMMENT '''
                                                                                    ELSE ''
                                                                                    END || nvl(REMARKS,
                                                                                               '') ||
                                                                                CASE
                                                                                    WHEN REMARKS IS NOT NULL THEN ''''
                                                                                    ELSE ''
                                                                                    END,
                                                                                ', ') WITHIN GROUP (
                                                                                    ORDER BY
                                                                                        SRC_COLUMN_POS) lstD
                                                         FROM v_table_column_all_distinct v
                                                         WHERE v.CORE_PARTITION_POS IS NULL
                                                           AND v.USE_CORE = TRUE
                                                         GROUP BY TABLE_ID,
                                                                  TABLE_NAME_DWH),
                                                     PARTITION AS (
                                                         SELECT v.TABLE_ID,
                                                                ' PARTITIONED BY (' ||
                                                                listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
                                                                        CASE
                                                                            WHEN REMARKS IS NOT NULL THEN ' COMMENT '''
                                                                            ELSE ''
                                                                            END || nvl(REMARKS,
                                                                                       '') ||
                                                                        CASE
                                                                            WHEN REMARKS IS NOT NULL THEN ''''
                                                                            ELSE ''
                                                                            END,
                                                                        ', ') WITHIN GROUP (
                                                                            ORDER BY
                                                                                CORE_PARTITION_POS) || ') ' lst
                                                         FROM TABLE_REPO.v_table_column_all v
                                                         WHERE v.CORE_PARTITION_POS IS NOT NULL
                                                         GROUP BY TABLE_ID),
                                                     head AS (
                                                         SELECT t.table_id,
                                                                'CREATE TABLE IF NOT EXISTS  ' || t.DATABASE_CORE ||
                                                                '.' ||
                                                                LOWER(t.source_system || '_' || t.TABLE_NAME) head
                                                         FROM table_repo.table t
                                                         WHERE t.USAGE_FLAG = true and DATABASE_CORE is not null and DATABASE_CORE <> ''),
                                                     pk AS (
                                                         SELECT v.TABLE_ID,
                                                                ', primary key(' ||
                                                                listagg(CAST(v.COLUMN_NAME AS VARCHAR(30000)),
                                                                        ', ') WITHIN GROUP (
                                                                            ORDER BY
                                                                                CORE_SID_COL) ||
                                                                ') disable novalidate )' pk
                                                         FROM TABLE_REPO.v_table_column_all v
                                                         WHERE v.CORE_ID_COL = TRUE
                                                           AND v.CORE_PARTITION_POS IS NULL
                                                         GROUP BY TABLE_ID),
                                                     tail AS (
                                                         SELECT table_id,
                                                                ' STORED AS ' || t.FILE_TYPE_CORE ||
                                                                ' TBLPROPERTIES (''PARQUET.COMPRESS''=''SNAPPY'')' tail
                                                         FROM table_repo.table t),
                                                     COMMENT AS (
                                                         SELECT table_id,
                                                                CASE
                                                                    WHEN t.remarks IS NOT NULL
                                                                        AND LENGTH(t.remarkS) > 0
                                                                        THEN ' COMMENT ''' || t.REMARKs || ''''
                                                                    ELSE ''
                                                                    END COMMENT
                                                         FROM table_repo.table t)
                                                SELECT a.table_id,
                                                       b.table_name_dwh,
                                                       a.head,
                                                       b.lstD,
                                                       c.lst,
                                                       d.pk,
                                                       t.tail,
                                                       e.comment,
                                                       a.head || b.lstD || d.pk || e.comment || c.lst || t.tail || ';' ddl
                                                FROM head a
                                                         LEFT JOIN COLUMN b ON
                                                    (a.table_id = b.table_id)
                                                         LEFT JOIN PK d ON
                                                    (a.TABLE_ID = d.TABLE_ID)
                                                         LEFT JOIN PARTITION c ON
                                                    (c.TABLE_ID = a.table_id)
                                                         LEFT JOIN tail t ON
                                                    (t.table_id = a.table_id)
                                                         LEFT JOIN COMMENT e ON
                                                    (e.table_id = a.table_id);


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE VIEW table_repo.v_ddl_data_mart AS  WITH v_table_column_all_distinct
AS (
    SELECT DISTINCT table_id,
                    table_name_dwh,
                    remarks,
                    src_column_pos,
                    ddl_line,
                    core_partition_pos,
                    use_core,
                    USE_DM
    FROM TABLE_REPO.v_table_column_all),
                                                     COLUMN AS (
                                                         SELECT v.TABLE_ID,
                                                                v.TABLE_NAME_DWH,
                                                                ' (' || listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
                                                                                CASE
                                                                                    WHEN REMARKS IS NOT NULL
                                                                                        THEN ' COMMENT '''
                                                                                    ELSE ''
                                                                                    END || nvl(REMARKS,
                                                                                               '') ||
                                                                                CASE
                                                                                    WHEN REMARKS IS NOT NULL THEN ''''
                                                                                    ELSE ''
                                                                                    END,
                                                                                ', ') WITHIN GROUP (
                                                                                    ORDER BY
                                                                                        SRC_COLUMN_POS) lstD
                                                         FROM v_table_column_all_distinct v
                                                         WHERE v.CORE_PARTITION_POS IS NULL
                                                           AND v.use_dm = TRUE
                                                         GROUP BY TABLE_ID,
                                                                  TABLE_NAME_DWH),
                                                     PARTITION AS (
                                                         SELECT v.TABLE_ID,
                                                                ' PARTITIONED BY (' ||
                                                                listagg(CAST(v.DDL_LINE AS VARCHAR(30000)) ||
                                                                        CASE
                                                                            WHEN REMARKS IS NOT NULL THEN ' COMMENT '''
                                                                            ELSE ''
                                                                            END || nvl(REMARKS,
                                                                                       '') ||
                                                                        CASE
                                                                            WHEN REMARKS IS NOT NULL THEN ''''
                                                                            ELSE ''
                                                                            END,
                                                                        ', ') WITHIN GROUP (
                                                                            ORDER BY
                                                                                CORE_PARTITION_POS) || ') ' lst
                                                         FROM TABLE_REPO.v_table_column_all v
                                                         WHERE v.CORE_PARTITION_POS IS NOT NULL
                                                         GROUP BY TABLE_ID),
                                                     head AS (
                                                         SELECT t.table_id,
                                                                'CREATE TABLE IF NOT EXISTS  ' || t.DATABASE_DATAMART ||
                                                                '.' ||
                                                                LOWER(t.source_system || '_' || t.TABLE_NAME) head
                                                         FROM table_repo.table t
                                                         WHERE t.USAGE_FLAG = true and DATABASE_DATAMART is not null and DATABASE_DATAMART <> ''),
                                                     pk AS (
                                                         SELECT v.TABLE_ID,
                                                                ', primary key(' ||
                                                                listagg(CAST(v.COLUMN_NAME AS VARCHAR(30000)),
                                                                        ', ') WITHIN GROUP (
                                                                            ORDER BY
                                                                                CORE_SID_COL) ||
                                                                ') disable novalidate )' pk
                                                         FROM TABLE_REPO.v_table_column_all v
                                                         WHERE v.CORE_ID_COL = TRUE
                                                           AND v.CORE_PARTITION_POS IS NULL
                                                         GROUP BY TABLE_ID),
                                                     tail AS (
                                                         SELECT table_id,
                                                                ' STORED AS ' || t.FILE_TYPE_CORE ||
                                                                ' TBLPROPERTIES (''PARQUET.COMPRESS''=''SNAPPY'')' tail
                                                         FROM table_repo.table t),
                                                     COMMENT AS (
                                                         SELECT table_id,
                                                                CASE
                                                                    WHEN t.remarks IS NOT NULL
                                                                        AND LENGTH(t.remarkS) > 0
                                                                        THEN ' COMMENT ''' || t.REMARKs || ''''
                                                                    ELSE ''
                                                                    END COMMENT
                                                         FROM table_repo.table t)
                                                SELECT a.table_id,
                                                       b.table_name_dwh,
                                                       a.head,
                                                       b.lstD,
                                                       c.lst,
                                                       d.pk,
                                                       t.tail,
                                                       e.comment,
                                                       a.head || b.lstD || nvl(d.pk,'') || e.comment || nvl(c.lst,'') || t.tail || ';' ddl
                                                FROM head a
                                                         LEFT JOIN COLUMN b ON
                                                    (a.table_id = b.table_id)
                                                         LEFT JOIN PK d ON
                                                    (a.TABLE_ID = d.TABLE_ID)
                                                         LEFT JOIN PARTITION c ON
                                                    (c.TABLE_ID = a.table_id)
                                                         LEFT JOIN tail t ON
                                                    (t.table_id = a.table_id)
                                                         LEFT JOIN COMMENT e ON
                                                    (e.table_id = a.table_id);


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.v_docu_dm as
with s1 as (
    select t.DATABASE_DATAMART DATABASE,
           lower(t.TABLE_NAME) TABLE,
           c.COLUMN,
           ROW_NUMBER() OVER (partition by table_name order by SRC_COLUMN_POS  ) COLUMN_POS,
           c.DWH_TYPE TYPE,
           c.DWH_LENGTH LENGTH,
           c.DWH_SCALE SCALE,
           c.REMARKS REMARKS
    from TABLE_REPO.TABLE t
             join TABLE_REPO.V_TABLE_COLUMN_ALL c on (c.table_id = t.table_id)
    where t.USAGE_FLAG = true
      and c.USE_DM = true

)
select *
from s1;


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.V_DDL_DATA_MART_ACCESS_VIEW as
with v_table_column_all_distinct as (select distinct table_id, table_name_dwh, column_name,remarks, src_column_pos, ddl_line, core_partition_pos, use_core, USE_DM from TABLE_REPO.v_table_column_all),
     access_view as (select v.table_id,
                            'CREATE OR REPLACE VIEW ' || t.DATABASE_DM_VIEW || '.v_' || lower(t.source_system || '_' || t.TABLE_NAME) ||
                            ' AS SELECT ' ||
                            listagg(lower(nvl(v.COLUMN_NAME, v.COLUMN_NAME)), ',')
                                    within group ( order by SRC_COLUMN_POS) || ' FROM ' || t.DATABASE_DATAMART || '.' ||
                            lower(t.source_system || '_' || t.TABLE_NAME || ';') ddl
                     from v_table_column_all_distinct v
                              join table_repo.table t on (t.table_id = v.table_id)
                     where use_dm = true
                     group by v.table_id, t.DATABASE_DATAMART, t.TABLE_NAME,t.DATABASE_DM_VIEW, t.SOURCE_SYSTEM
)
select *
from access_view a;


SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view table_repo.v_spark_schema as
with v_table_column_all_distinct as (select distinct table_id,
                                                     table_name_dwh,
                                                     column_name,
                                                     remarks,
                                                     src_column_pos,
                                                     ddl_line,
                                                     core_partition_pos,
                                                     use_core,
                                                      case  lower(dwh_type)
                                    when 'string'then 'StringType()'
                                    when  'integer' then  'IntegerType()'
                                    when  'timestamp' then  'TimestampType()'
                                    else 'ERROR'
                                end spark_type
                                     from TABLE_REPO.v_table_column_all),
     access_view as (select v.table_id,
                            ' StructType([StructField(''' ||
                            listagg(lower(nvl(cast(v.COLUMN_NAME as varchar(20000)), v.COLUMN_NAME))|| ''',' || spark_type
                                , '),StructField(''')
                                    within group ( order by SRC_COLUMN_POS)  || ')])' ddl
                     from v_table_column_all_distinct v
                              join table_repo.table t on (t.table_id = v.table_id)
                     group by v.table_id, t.DATABASE_CORE, t.TABLE_NAME, t.DATABASE_VIEW, t.SOURCE_SYSTEM
     )
select *
from access_view a;







SET CURRENT SCHEMA = "DB2INST1";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.v_cognos_tabellen_spalten_join AS
SELECT t1.beschreibung_tabelle, t1.TABELLEN_NAME, t2.TABELLEN_NAME_DWH, t1.DATENSATZ_ID, t2.DATENTYP, t1.PRIMARY_KEY, t1.QUELLSCHEMA, t1.QUELLSYSTEM,
t2.beschreibung, t2.Feld_name, t2.ID_FELD, t2.LÄNGE, t2.PRIMARY_KEY_FELD, t2."SCALE"
FROM TABLE_REPO.v_cognos_tabellen t1
JOIN
TABLE_REPO.v_cognos_tabellen_spalten t2
ON t2.TABLE_ID = t1.TABLE_ID;















SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view table_repo.v_spark_schema as
with v_table_column_all_distinct as (select distinct table_id,
                                                     table_name_dwh,
                                                     column_name,
                                                     remarks,
                                                     src_column_pos,
                                                     ddl_line,
                                                     core_partition_pos,
                                                     use_core,
                                                      case  lower(dwh_type)
                                    when 'string'then 'StringType()'
                                    when  'integer' then  'IntegerType()'
                                    when  'timestamp' then  'TimestampType()'
                                    else 'ERROR'
                                end spark_type
                                     from TABLE_REPO.v_table_column_all),
     access_view as (select v.table_id,
                            ' StructType([StructField(''' ||
                            listagg(lower(nvl(cast(v.COLUMN_NAME as varchar(20000)), v.COLUMN_NAME))|| ''',' || spark_type
                                , '),StructField(''')
                                    within group ( order by SRC_COLUMN_POS)  || ')])' ddl
                     from v_table_column_all_distinct v
                              join table_repo.table t on (t.table_id = v.table_id)
                     group by v.table_id, t.DATABASE_CORE, t.TABLE_NAME, t.DATABASE_VIEW, t.SOURCE_SYSTEM
     )
select *
from access_view a;



SET CURRENT SCHEMA = "TABLE_REPO";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.v_cognos_tabellen as
select lower(table_id)      table_id,
       lower(source_system) Quellsystem,
       lower(table_name)    Tabellen_name,
       database_view        Quellschema,
       lower(sid_column)    Primary_Key,
       lower(id_column)     Datensatz_id,
       remarks              Beschreibung_tabelle
from TABLE_REPO.table
where usage_flag = true;


SET CURRENT SCHEMA = "DB2INST1";
SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","DB2INST1";
CREATE view TABLE_REPO.v_cognos_tabellen_spalten_join AS
SELECT t1.beschreibung_tabelle, t1.TABELLEN_NAME, t2.TABELLEN_NAME_DWH, t1.DATENSATZ_ID, t2.DATENTYP, t1.PRIMARY_KEY, t1.QUELLSCHEMA, t1.QUELLSYSTEM,
t2.beschreibung, t2.Feld_name, t2.ID_FELD, t2.LÄNGE, t2.PRIMARY_KEY_FELD, t2."SCALE"
FROM TABLE_REPO.v_cognos_tabellen t1
JOIN
TABLE_REPO.v_cognos_tabellen_spalten t2
ON t2.TABLE_ID = t1.TABLE_ID;






COMMIT WORK;

CONNECT RESET;

TERMINATE;

