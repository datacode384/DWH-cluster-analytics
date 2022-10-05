"""
usage: 
1. client mode 
spark-submit --master yarn --deploy-mode client --files config.json spark_count_words_2ndIteration.py
2. cluster mode
spark-submit --master yarn --deploy-mode cluster --files config.json spark_count_words_2ndIteration.py

Author: krishna damarla
"""

import sys, os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import json

if __name__ == "__main__":

    data = json.load(open("config.json","r"))

    for x in data["spark"]:
        #spark = SparkSession.builder.config(x,data["spark"][x]).master("spark://10.85.52.12:7077").enableHiveSupport().getOrCreate()
         spark = SparkSession.builder.config(x,data["spark"][x]).enableHiveSupport().getOrCreate()

    sc = spark.sparkContext
    
    os.system("sh copy-file-to-hdfs.sh")

    words = sc.textFile("./shakespeare.txt").flatMap(lambda line: line.split(" "))

    # count the occurrence of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

    for x in wordCounts.collect():
        print(x)

    #save the counts to output
    #wordCounts.saveAsTextFile("/user/spark/wordCount_output.txt",  mode='overwrite')
                                                                                       
