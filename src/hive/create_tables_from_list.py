from hdfs_functions import *

tables =['bom','enum','enumattr','pd','pdtf','pw','risktype','tf','version']

for table in tables:
	#l, p, error = run_cmd(['hive','-f','raw_zone/pas_'+table+'_raw_zone.hql'])
	#print(l,p,error)
	l, p, error = run_cmd(['hive','-f','core/pas_'+table+'_core.hql'])
	print(l,p,error)
