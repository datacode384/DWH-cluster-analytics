import datetime
import ibm_db as db2
from connect import connect_db02

con = connect_db02()


tables = ["BEARBNW", "HVGRUPPE", "JURLV", "PRV", "RISSCHAETZ", "STEUSCHICHT", "SNTVT", "VB", "ZAHLEMPF", "VP", "BEGUENST", "BEITRAGSVEKTOR", "JURVT", "POLDARL", "PROVDATEN", "RISMERKMAL", "NOTIZ", "SKFONDSANTEIL", "SKUEBVERW", "STEUWERTE", "STEUWERTESTAND", "VTPFLEGE", "VTTA", "VERTSCHL", "VPVT", "AKTSTAND", "ANTRABW", "BTGAUFTREG", "DRITTRECHT", "EAKLAUSEL", "JAHRESWERTE", "JAHRWERTEINFOFONDS", "KTOFONDS", "KTOSTD", "NBSMAPPINGINTEXT", "SBBSTDBWG", "SKBILWERT", "SBLEISTWERT", "SNTVTLSTG", "STEUWERTEKUM", "TRANSPARENZ", "VBLSTG", "WERTVB", "VTLSTG", "KINDERZULAGE", "LV", "LVSTAND", "ABSCHLKO","ANREWERTE","BEARBNWEXT","GARWERTVORGABE","JAHRWERTEINFO","SBBZ","SBINKASSO","SBQUOTAUSGL","SBSTICHBWG","SBUEBVERW","SBUEBZU","SBVERZANS","SKQUOTAUSGL","SKSCHADENRES","SKSGA","SKUEBZU","SKVORAB","SRES","SRESAGGSKBILWERT","SRESEINZEL","STEUAUFTREG","STEUWERTEAUSZAUFT","STEUWERTEUEB","TRANSPARENZGARKO","VBEXT","VBUEBVERW","VERSAUSGL","VERSAUSGLENTN","ZAHLEMPFDAT", "VT"]

#tables = ["NBSMAPPINGINTEXT"]

csv_path = "../../data/lfdb_data_01_2020/"
unit_test_data = "../../data/unit_test_data/"
sql = "select dwh_type from table_repo.v_table_column_all where table_id = ? and column_name = ?"

for table in tables:
	table_id = "LF_EBF_" + table
	csv_path_full = csv_path + table.lower() + ".csv"
	data_path = unit_test_data + table.lower() + ".csv"
	with open(csv_path_full) as f:
		columns = f.readline().strip().split(",")
		data = f.readline().strip().split(",")
		data2 = f.readline().strip().split(",")
	i = 0
	if len(data) >1:
		while i<len(data):
			if data[i] is None or data[i] is '' or data[i] is '""' or data[i] is "":
				stmt = db2.prepare(con, sql)
				db2.bind_param(stmt, 1, table_id)
				db2.bind_param(stmt, 2, columns[i].lower().strip('"'))
				db2.execute(stmt)
				btype = db2.fetch_assoc(stmt)
				if btype != False:
					dtype = btype["DWH_TYPE"]
					if dtype == 'timestamp':
						data[i] = '2019-01-01'
					elif dtype == 'string':
						data[i] = '"test"'
					elif dtype == 'integer' or dtype == 'smallint':
						data[i] = str(100)
					elif dtype == 'decimal':
						data[i] = str(1.1)
				else:
					print(table)
					print(columns[i].lower().strip('"'))
			if data2[i] is None or data2[i] is '' or data2[i] is '""' or data2[i] is "":
				stmt = db2.prepare(con, sql)
				db2.bind_param(stmt, 1, table_id)
				db2.bind_param(stmt, 2, columns[i].lower().strip('"'))
				db2.execute(stmt)
				btype = db2.fetch_assoc(stmt)
				if btype != False:
					dtype = btype["DWH_TYPE"]
					if dtype == 'timestamp':
						data2[i] = '2019-02-02'
					elif dtype == 'string':
						data2[i] = '"test2"'
					elif dtype == 'integer' or dtype == 'smallint':
						data2[i] = str(200)
					elif dtype == 'decimal':
						data2[i] = str(2.2)
					else:
						print(table)
						print(columns[i].lower().strip('"'))
			i+=1
	else:
		data = []
		data2 = []
		while i<len(columns):
			stmt = db2.prepare(con, sql)
			db2.bind_param(stmt, 1, table_id)
			db2.bind_param(stmt, 2, columns[i].lower().strip('"'))
			db2.execute(stmt)
			btype = db2.fetch_assoc(stmt)
			if btype != False:
				dtype = btype["DWH_TYPE"]
				if dtype == 'timestamp':
					data.append('2019-01-01')
					data2.append('2019-02-02')
				elif dtype == 'string':
					data.append('"test"')
					data2.append('"test2"')
				elif dtype == 'integer' or dtype == 'smallint':
					data.append(str(100))
					data2.append(str(200))
				elif dtype =='decimal':
					data.append(str(1.1))
					data2.append(str(2.2))
			i+=1
	columns = ",".join(columns)
	data = ",".join(data)
	data2 = ",".join(data2)
	with open(data_path, "w") as w:
		w.write(columns + "\n")
		w.write(data +"\n")
		w.write(data2)
