"""
This program tests the functions from job_monitoring_functions.py file

usage: python -m unittest job_monitoring_functions.py

"""

import unittest
import sys

sys.path.append('../python')
import job_monitoring_functions

class TestJobMonitoring(unittest.TestCase):
	
	def test_start_job_execution1(self):
		result = job_monitoring_functions.getProcessID("ergo","dwh", "lv_ingest_ergo_dwh")	
		self.assertEqual(result, 4)
		
	def test_start_job_execution2(self):
		result = job_monitoring_functions.getProcessID("ergo","dwh", "vt_ingest_ergo_dwh")
		self.assertNotEqual(result, None)

	def test_start_job_execution3(self):
		result = job_monitoring_functions.getProcessID("ergo","DWH","hvgruppe_ingest_ergo_dwh")
		self.assertEqual(result, None)
	
	def test_start_job_execution4(self):
		result = job_monitoring_functions.getProcessID("ERGO","dwh","hvgruppe_ingest_ergo_dwh")
		self.assertEqual(result, None)
	
	def test_start_job_step_execution1(self):
		getProcessStepID = job_monitoring_functions.getProcessStepID(1)
		result = job_monitoring_functions.startJobStepExecution(1,1)
		self.assertEqual(result, getProcessStepID + 1)
		job_monitoring_functions.deleteRowJobStepExecution(1)

	def test_set_status_job_step_execution(self):
		jobStatus = job_monitoring_functions.getStepJobStatus(1,1)
		print(jobStatus)
		updatedStatusJobStepExecution = job_monitoring_functions.setStatusJobStepExecution(1,1,"stopped")
		print(updatedStatusJobStepExecution)
		job_monitoring_functions.setStatusJobStepExecution(1,1,jobStatus)
		

