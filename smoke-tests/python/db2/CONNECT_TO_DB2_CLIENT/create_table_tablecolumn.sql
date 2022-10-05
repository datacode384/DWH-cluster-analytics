-- TABLE_REPO."TABLE" definition

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
		  "HIST_TYPE" VARCHAR(100 OCTETS) )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW
 ;

COMMENT ON TABLE "TABLE_REPO"."TABLE" IS 'Contains definition of all tables used in the system. Definitions are extracted from db2 catalog of different source systems(lf, sap, bpms). db2 system catalog have all metadata info. This was exported directly int this table'
;
ALTER TABLE "TABLE_REPO"."TABLE" 
	ADD CONSTRAINT "TABLE_PK" PRIMARY KEY
		("TABLE_ID")
;

ALTER TABLE "TABLE_REPO"."TABLE" 
	ADD CONSTRAINT "TABLE_UK" UNIQUE
		("SOURCE_SYSTEM",
		 "TABLE_SCHEMA",
		 "TABLE_NAME")
;

-- TABLE_REPO.TABLE_COLUMN definition

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
		  "MESSAGE_FILTER" BOOLEAN NOT NULL WITH DEFAULT FALSE )   
		 IN "TSN_REG_DB01"  
		 ORGANIZE BY ROW
 ;

COMMENT ON TABLE "TABLE_REPO"."TABLE_COLUMN" IS 'Contains all columns used in tables. Metadata '
;
ALTER TABLE "TABLE_REPO"."TABLE_COLUMN" 
	ADD CONSTRAINT "TABLE_COLUMN_PK" PRIMARY KEY
		("TABLE_ID",
		 "SRC_COLUMN_NAME")
;
