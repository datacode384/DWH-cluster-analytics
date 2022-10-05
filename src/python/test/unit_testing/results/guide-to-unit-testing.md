```
Guide to unit testing 

Author: Krishna Damarla
```

# Deequ 

[Deequ](https://dl.acm.org/citation.cfm?id=3320210) is a library built on top of Apache Spark for defining "unit tests for data", which measure data quality in large datasets.

Manual data type conversion is error prone and time consuming (we have already seen such case here  https://lifeinsuranceplatform.slack.com/archives/GMDGM4ZTP/p1573142576066500).

We could do automatic data type conversion as suggested [here](
https://towardsdatascience.com/automated-data-quality-testing-at-scale-using-apache-spark-93bb1e2c5cd0 ) from db2 to hive using spark jdbc plugin. 

We have already implemented such code here https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/src/remote/python/load_db2_table_to_hive.py 

However, its in python. If we can replicate the same code to scala, we could do unit testing for data easily using deequ. 

This would solve many of our current / future problems we discussed in todays unit testing call.

Please kindly let me know your suggestions / ideas.

# References
- Automating Large-Scale Data Quality Verification - http://www.vldb.org/pvldb/vol11/p1781-schelter.pdf
- unit testing data with deequ - https://github.com/awslabs/deequ
- Other External data validation testing for big data tools - https://www.xenonstack.com/insights/what-is-data-validation-testing/
