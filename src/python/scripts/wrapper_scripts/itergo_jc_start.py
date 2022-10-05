import sys
sys.path.append("..")
from monitoring.job_monitoring_functions import *

def jc_start():
	application_name = "DWH"
	tenant_name = "ITERGO"
	pid = start_job_execution(tenant_name.lower(), application_name.lower())
	
if __name__=="__main__":
	jc_start()
