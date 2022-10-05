"""
This program tests the functions from connect.py file

usage: python -m unittest connect_tests.py

"""

import unittest 
import sys

sys.path.append('../python')
import connect

class TestConnect(unittest.TestCase):
	def test_make_connection(self):
		result = connect.make_connection("10.85.200.175","IBM DB2 ODBC DRIVER - C_clidriver","50000","db01","dbuser1","Xbv7J6A8RdqkP8mV","30")
		self.assertNotEqual(result,"test")		
		
