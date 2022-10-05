from hdfs_functions import *
tables =["bnka","but000","but020","but0bk","but0id","tiban"]

l, p, error = run_cmd(['python','itergo_jc_start.py'])
print(l,p,error)
if 'bnka' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/par_bnka_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/par_bnka_wrapper_core.py'])
	print(l,p,error)
if 'but000' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/par_but000_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/par_but000_wrapper_core.py'])
	print(l,p,error)
if 'but020' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/par_but020_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/par_but020_wrapper_core.py'])
	print(l,p,error)
if 'but0bk' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/par_but0bk_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/par_but0bk_wrapper_core.py'])
	print(l,p,error)
if 'but0id' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/par_but0id_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/par_but0id_wrapper_core.py'])
	print(l,p,error)
if 'tiban' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/par_tiban_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/par_tiban_wrapper_core.py'])
	print(l,p,error)
l,p,error = run_cmd(['python','itergo_jc_stop.py'])
