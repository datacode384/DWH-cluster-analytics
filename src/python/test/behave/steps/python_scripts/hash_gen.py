"""
Usage: from hash_gen import name_of_function
"""

from pyspark.sql.functions import sha2, concat_ws, hash, md5

def hash_hash(array):
	return hash(concat_ws("-",*array))

def md5_hash(array):
	return md5(concat_ws("-",*array))

def sha2_hash():
	return sha2(concat_ws("-",*array),256)
