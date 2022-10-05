-- job_repo tables description

COMMENT ON TABLE JOB_REPO.DATABASE_PARAMETER
	IS 'Defines Hive databases for each tenant (itergo). This table is obsolete and have to be replaced/Need modifications. ';
	
COMMENT ON TABLE JOB_REPO.ENVIRONMENT
	IS 'Reference table that defines stages  avaialble in system.  For ex, production DEV. ';

COMMENT ON TABLE JOB_REPO.ENVIRONMENT_TENANT
	IS 'Defines which tenants(itergo, or other test tenants) are used in environment(dev/sit/cit). Test to be modified.  '; 

COMMENT ON TABLE JOB_REPO.JOB
	IS 'Defines jobs that are executed by IWS. For every parameter_group_id, we linnk corresponing parameter_group_value_id . 
	For example, the result concentation of parameter_group_id: lv_core  is  lv_core_vt, lv_core_hvgruppe etc  '; 

COMMENT ON TABLE JOB_REPO.JOB_CHAIN
	IS 'Defines attributes(jc_start, jc_stop) of the job_chain. A job_chain gets all the jobs to load to raw_zone / core. 
		Currently we have only one job_chain PROD. '; 

COMMENT ON TABLE JOB_REPO.JOB_CHAIN_PARAMETER
	IS 'Defines parameters for all jobs within the job_chain. Ex: current 13 parameters in the data'; 

COMMENT ON TABLE JOB_REPO.JOB_DEPENDENCY
	IS 'Defines dependencies between jobs. Child jobs are executed if all parent jobs have been finished successfully. 
		Ex: filter child_id with jc_stop. U get 5 parent jobs. '; 

COMMENT ON TABLE JOB_REPO.JOB_GROUP
	IS 'This table is used for gropping jobs. 
		Ex: ALL jobs that are used for loading data to core datbase belong to one job group. '; 

COMMENT ON TABLE JOB_REPO.JOB_GROUP_PARAMETER
	IS 'Defines  parameters and their values avaialble for all jobs within job groups. 
		For example, to add table prefix pas, par, we get extension_type values as pas in data (current vaule is cp) '; 

COMMENT ON TABLE JOB_REPO.JOB_PARAMETER
	IS 'Deines parameter values specific to a job within the job group. '; 

COMMENT ON TABLE JOB_REPO."PARAMETER"
	IS 'Reference table that defines all parameters used in job_group_parameter, job_paramter, job_chain_paramter for generating jobs.'; 

COMMENT ON TABLE JOB_REPO.PARAMETER_GROUP
	IS 'Reference table that defines paramter groups used in table job and table parameter_group_value'; 

COMMENT ON TABLE JOB_REPO.PARAMETER_GROUP_VALUE
	IS 'Contains set of values for each parameter group For ex: all tables of a paramter group b. '; 

COMMENT ON TABLE JOB_REPO.SCRIPT	
	IS 'Code script template for core tables with differnet paramters. 
		These parameter values are set in v_job_paramter and v_generated_scripts. '; 

COMMENT ON TABLE JOB_REPO.TENANT
	IS 'Reference table for tenants'; 


-- job_repo views description

COMMENT ON TABLE JOB_REPO.V_ENVIRONMENT_TENANT 
     IS 'Joins table env & env_tenant. Provides parameteres used for creating wrapper scripts';
     
COMMENT ON TABLE JOB_REPO.V_GENERATED_SCRIPTS 
     IS 'Generates wrapper scripts without substitution of parameters env and tenant. But all other paramters are substituted';
     
COMMENT ON TABLE JOB_REPO.V_GENERATED_SCRIPTS_FINAL 
     IS 'Substitutes paramaters env & tenant in v_generated_scripts';
     
COMMENT ON TABLE JOB_REPO.V_IWS_DOCU_JOB_PARAMETER 
     IS 'Used for providing documentation for batch team. This view is still in construction. Need other parameters update';
     
COMMENT ON TABLE JOB_REPO.V_IWS_JOB_CHAIN_FINAL 
     IS 'Generates xml definition of IWS job chain. Will be used by batch team to create job chain';
     
COMMENT ON TABLE JOB_REPO.V_IWS_JOB_CHAIN_TENANT 
     IS 'Generates xml definition of IWS job chain. Will be used by batch team to create job chain';
     
COMMENT ON TABLE JOB_REPO.V_JOBS
     IS 'Used for documentation purposes to store all job names ';
     
COMMENT ON TABLE JOB_REPO.V_JOB_DEPENDENCIES
     IS ' Adds the table name from the job_dependency table. Child job starts after finishing parent job. 
    		Ex: Core Integration(CI) starts after Rawvault Integration(RI) ';
     
COMMENT ON TABLE JOB_REPO.V_JOB_DEPENDENCIES_FINAL 
     IS ' Adds the Tenant name(itergo) to V_JOB_DEPENDECIES. 
    		Child job starts after finishing parent job. Ex: Core Integration(CI) starts after Rawvault Integration(RI) ';
     
COMMENT ON TABLE JOB_REPO.V_JOB_PARAMETER 
	 IS 'Describes paramters for all jobs. This view is an input source for view v_generated_scripts';
	 
 COMMENT ON TABLE JOB_REPO.V_JOB_PARAMETER_ENV_TEN 
	 IS 'Only for checking errors in v_job_paramter, we are adding env and ten as seperate parameter columns in this view. 
	 	Other columns are sames as in v_job_paramter. ';
	 
 COMMENT ON TABLE JOB_REPO.V_TEC_JOBS 
 	IS 'Contains all jobs used in job chain after resolving paramter group dependecies. 
 		For example, paramter group is lf / sap / bpms etc';
 	
