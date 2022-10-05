 -- table_repo tables description
 
COMMENT ON TABLE TABLE_REPO.DATABASES  
   	IS 'Table is not used anymore';
   
COMMENT ON TABLE TABLE_REPO."JOIN" 
   	IS 'Reference table for joins between tables'; 
  
COMMENT ON TABLE TABLE_REPO.JOIN_2_TABLE 
   	IS 'Defines joins that are used for loading data into the core. 
		Table_id has the table name and join_id has the tables that needs to be joined to table_id. 
		2 or more tables can be used for joining.'; 

COMMENT ON TABLE TABLE_REPO.JOIN_COLUMN 
	IS 'Contains the columns  of the join_id that are joined to the target tables ';

COMMENT ON TABLE TABLE_REPO.JOIN_CONDITION 
	IS 'Defines on what columns we have to define the join condition.For example, slect *from tanb1 join tab2 on tab1.lvid = tab2.lvid';

COMMENT ON TABLE TABLE_REPO."TABLE" 
	IS 'Contains definition of all tables used in the system. Definitions are extracted from db2 catalog of different source systems(lf, sap, bpms). 
		db2 system catalog have all metadata info. This was exported directly int this table';
	
COMMENT ON TABLE TABLE_REPO.TABLE_COLUMN 
	IS 'Contains all columns used in tables. Metadata ';

COMMENT ON TABLE TABLE_REPO.TABLE_REPO_FOREIGN_KEY 
	IS 'Defines foreign key realtionship between tables. Table_id foregin key is in table_ref_id';

COMMENT ON TABLE TABLE_REPO.TABLE_SHARED_COLUMN 
	IS 'Contains additional technical columns used in multiple tables. 
		Table_group_id takes the 5 column_names defined for that specific table_group_id';
	
COMMENT ON TABLE TABLE_REPO.TEST 
	IS 'Table not needed';

-- table_repo views description

COMMENT ON TABLE TABLE_REPO.V_DDL_CORE 
     IS 'Provides ddl stmts for creating core tables';

COMMENT ON TABLE TABLE_REPO.V_DDL_CORE_ACCESS_VIEW  
     IS 'provides access views FOR core tables. This VIEW takes all columns from core tables. 
     	Access views are used by end users for accesing core tables. This is due to security reasons';
     
COMMENT ON TABLE TABLE_REPO.V_DDL_CORE_HIST 
	 IS 'Adds temporara atteibutes to access views For ex: valid_from / valid_to columns';
	 
 COMMENT ON TABLE TABLE_REPO.V_DDL_CORE_KEY
	 IS 'Contains ddl stmts for creating key tables';
 
 COMMENT ON TABLE TABLE_REPO.V_DDL_CORE 
     IS 'Provides ddl stmts for creating rawvault tables';
     
 COMMENT ON TABLE TABLE_REPO.V_DOCU_CORE 
     IS 'Provides docu of core data model to be used in confluence. Defines all fields that are available in core tables. 
		Technical fileds like Process_id, diff_hash are added here ' ;
    
 COMMENT ON TABLE TABLE_REPO.V_DOCU_INTERFACE 
     IS 'Provides docu about data provided by source system to be used in confluence. 
		Defines all fields of source system tables used in thw dwh. Doesnot have any technical fields';

COMMENT ON TABLE TABLE_REPO.V_DOCU_RAW_VAULT 
     IS 'Contains definition of all raw_zone tables. All these defintions are extracted from db2 catalog';
    
COMMENT ON TABLE TABLE_REPO.V_DOCU_TABLES 
     IS 'Contains definition of only the tables of raw_zone / core. Not the complete column definition inside these tables';
    
COMMENT ON TABLE TABLE_REPO.V_DOCU_TABLES 
     IS 'We dont need this view';
     
COMMENT ON TABLE TABLE_REPO.V_TABLE_COLUMN_ALL 
     IS 'View provides all columns used in the definition of tables. 
		 Based on table_column but adds new fields (gueltig_ab, wirksam_ab, tpa_mandant from table_shared_column and join_column
     	 CHECK the use_interface ';
     	
 COMMENT ON TABLE TABLE_REPO.V_TABLE_DB2_EXTRACTION 
   	IS 'Still in construction. View has SQL stmts required for extracting data FROM db2 sources. Used for defining parameters in job_repo';
     
 COMMENT ON TABLE TABLE_REPO.V_TABLE_FOREIGN_KEY_PARAMETER 
   	IS 'Returns json definition of foregin key relationships for defining paraemters in job_repo. 
		For example, key_type_id and key_type_sid in v_job_parameter takes the values set in v_table_FOREIGN_KEY_PARAMETER';    
		
COMMENT ON TABLE TABLE_REPO.V_TABLE_JOIN 
   	IS 'Same as V_TABLE_FOREIGN_KEY_PARAMETER but with join condition for job_repo ';    
   	
COMMENT ON TABLE TABLE_REPO.V_TABLE_KEY_PARAMETER 
   	IS 'keys definition for job_repo';
   	
 COMMENT ON TABLE TABLE_REPO.ZTEST_TABLE_COLUMN_ALL 
   	IS 'To be deleted';
   
   
 
