import sys
sys.path.append("..")
from monitoring.job_monitoring_functions import *


def jc_stop():
	application_name = "DWH"
	pid = get_process_id(application_name.lower())
	set_status(pid, 'finished')

if __name__ == "__main__":
	jc_stop()
