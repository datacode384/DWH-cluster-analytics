"""
This program tests the functions from lv_ingest_ergo_dwh.py file

usage: python -m unittest lv_ingest_ergo_dwh_tests.py

"""

import unittest
import sys

sys.path.append('../python')
import lv_ingest_ergo_dwh

class TestIngest(unittest.TestCase):
        def test_ingest1(self):
		pid = lv_ingest_ergo_dwh.startJobExecution('ergo', 'dwh')
                result = lv_ingest_ergo_dwh.add_column('/shared/HIVE-Table-Data/LV.csv','')
                self.assertEqual(result, None)
	def 
