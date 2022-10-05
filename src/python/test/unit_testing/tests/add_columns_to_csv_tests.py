"""
This program tests the functions from job_monitoring_functions.py file

usage: python -m unittest add_column_to_csv_tests.py

"""

import unittest
import sys

sys.path.append('../python')
from add_columns_to_csv import add_column

class TestAddColumnsToCsv(unittest.TestCase):
	def test_add_column1(self):
		file = "VT_testfile.csv"
		result = add_column(file,'test','test')
		self.assertEqual(result, None)

	def test_add_column2(self):
		result = add_column('','test','test')
		print(result)

	def test_add_column3(self):
		file = "VT_testfile.csv"
		result = add_column(file,'','')
		self.assertEqual(result, None)
