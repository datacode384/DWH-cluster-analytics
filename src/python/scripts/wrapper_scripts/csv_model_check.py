#to run the script: spark-submit --jars db2jcc4.jar --properties-file conf/spark.conf csv_model_check.py >log

import datetime
from pyspark.sql import SparkSession
from spark_cfg import spark_init
from add_columns_to_csv import lower_col_names

tables =["abschlko","aktstand","anrewerte","antrabw","bearbnw","bearbnwext","beguenst","beitragsvektor","btgauftreg","drittrecht","eaklausel","garwertvorgabe","hvgruppe","jahreswerte","jahrwerteinfo","jahrwerteinfofonds","jurlv","jurvt","kinderzulage","ktofonds","ktostd","lv","lvstand","nbsmappingintext","notiz","partnerlv","poldarl","provdaten","prv","rismerkmal","risschaetz","sbbstdbwg","sbbz","sbinkasso","sbleistwert","sbquotausgl","sbstichbwg","sbuebverw","sbuebzu","sbverzans","skbilwert","skfondsanteil","skquotausgl","skschadenres","sksga","skuebverw","skuebzu","skvorab","sntvt","sntvtlstg","sres","sresaggskbilwert","sreseinzel","steuauftreg","steuschicht","steuwerte","steuwerteauszauft","steuwertekum","steuwertestand","steuwerteueb","transparenz","transparenzgarko","vb","vbext","vblstg","vbuebverw","versausgl","versausglentn","vertschl","vp","vpvt","vt","vtlstg","vtpflege","vtta","wertvb","zahlempf","zahlempfdat"]
#tables = ["lv", "bearbnw", "hvgruppe", "vt", "jurlv", "lv"]
spark = spark_init()
#user = "dbuser1"
#password = "Xbv7J6A8RdqkP8mV"
#jdbcurl = "jdbc:db2://10.85.200.175:50000/db01"
#user = "db2inst1"
#password = "password"
#jdbcurl = "jdbc:db2://10.85.200.151:50000/lfdb"
#driver = "com.ibm.db2.jcc.DB2Driver"
raw_zone = 'raw_zone'
user = "lfdwh"
password = "FEIRNffe0E95vRMRT"
jdbcurl ="jdbc:db2://10.85.200.160:50000/dispodb"
#user = "db2inst1"
#password = "3Hk3fVFQkd3LFCzc"
#jdbcurl = "jdbc:db2://10.85.200.180:50000/db01"
driver = "com.ibm.db2.jcc.DB2Driver"
#tables = ['bnka', 'but000', 'but020', 'but0bk', 'but0id', 'tiban']
for table in tables:
	tn = "pas_"+table
	#rawzone = spark.sql("select * from {0}.{1}".format(rawzone, tn.lower()))
	source = spark.read.format("jdbc")\
	.option('url', jdbcurl)\
	.option('driver', driver)\
	.option('user', user)\
	.option('dbtable','ebf.'+table)\
	.option('password', password)\
	.load()
	rawzone = spark.sql("select * from {0}.{1}".format(raw_zone, tn))
	#source.show():
	source = lower_col_names(source)
	missingDb2 = []
	technical =["process_id","insert_tst","source_system"]
	for col in rawzone.columns:
		if col not in source.columns and col not in technical:
			missingDb2.append(col)
	missingDWH = []
	for col in source.columns:
		if col not in rawzone.columns:
			missingDWH.append(col)
	if len(missingDb2)>0:
		print("For table " + table + " the following columns are in our datamodel, but not in db2: ", missingDb2)
	if len(missingDWH)>0:
		print("For " + table + " the following columns are in db2 but not in our datamodel: ", missingDWH)
