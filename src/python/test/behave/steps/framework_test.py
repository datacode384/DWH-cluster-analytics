from behave import given, when, then, step
import os
import copy
import numpy as np
from connect import connect_db2, connect_db02
import ibm_db as db2
from hdfs_functions import run_cmd
from impala.dbapi import connect

@given('db2 table "{table}" truncated')
def step_truncate_table(context, table):
    """
    Truncates a table in the database that keeps the DB2 monitoring tables
   """
    con = connect_db2()
    sql = 'delete from dbuser1.' + table
    st = db2.prepare(con,sql)
    db2.execute(st)

@given('dwh table "{table}" in "{database}" truncated')
def step_truncate_table(context, table, database):
    """
    Truncates table in DWH using Hive truncate table
    Function truncate_table_statement used for generating the SQL
    """
    #connect with impala as queries are faster with impala than hive	
    con = connect(host = 'dwh-hdp-node02.dev.ergo.liferunoffinsuranceplatform.com', port=21050)
    sql = 'truncate table ' + database + '.' + table
    st = db2.prepare(con, sql)
    db2.execute(st)
    #hql = 'truncate table ' + database + '.' + table
    #df = context.spark.sql(hql)

@given('file "{input_file}" created')
def step_framework_datafile_created(context, input_file):
    """
    Create a new Data file using the information in the context file.
    For each line in the context table, columns are overwritten.    
"""    
    path = "/shared/csv_files"
    write_file(path, input_file, context.table.headings, context.table)
    #write_file(path,input_file,"","")

@step("the following jobs are executed")
def step_impl(context):
    """
    Executes python scripts
"""    
    heading = context.table.headings[0]
    for row in context.table:
        run_cmd(['python', './steps/python_scripts/' + row[heading]])

@then('dwh table "{table}" in "{database}" contains')
def step_framework_table_contains(context, table, database):
    """
    Verifies whether table database.table contains the data as given in the context table
    table count in the context table refers to the number of records.
    Example: The context table contains in the feature script contains columns
    BEARBID = 2 and count = 2 then the condition is
    select Count(*) from database.table  where BEARBID = 2
    return 2
    """   
    expected =[]
    for heading in context.table.headings:
        expected.append([int(row[heading]) if row[heading].isdigit() else row[heading] for row in context.table])
    columns = ", ".join( repr(value) for value in context.table.headings).replace("'", "")
    actual = "select "+ columns + " from "+ database +"." + table
    df = context.spark.sql(actual)
    pdf=df.toPandas()
    actual = [(list(pdf[x])) for x in pdf.columns]
    np.testing.assert_array_equal(actual,expected)

@then('db2 table "{table}" contains')
def step_framework_job_table(context, table):
    """
    Verifies whether table contains the data as given in the context table.
    Queries database that is used for runtime monitoring
   """
    expected =[]
    for heading in context.table.headings:
        expected.append([(int(row[heading])) if row[heading].isdigit() else row[heading] for row in context.table])
    con = connect_db2()
    columns = ", ".join( repr(value) for value in context.table.headings).replace("'", "")
    actual = "select "+ columns + " from dbuser1." + table
    st = db2.prepare(con, actual)
    db2.execute(st)
    rows = db2.fetch_assoc(st)
    arr = [ list(row) for row in rows]
    if len(arr) == 1:
        ret = []
        for row in arr[0]:
            ret.append([row])
    else:
        for i in range(len(arr)):
            ret = [ list(x) for x in zip(*arr)]
    np.testing.assert_array_equal(ret, expected)

def write_file(path, file_name, header_lst, rows):
    """
    Creates a test file
    :param path: target directory
    :param file_name: name of the file
    :param header_lst: List with column names that will be modified
    :param rows: modified fields. Each line corresponts to one record
    :return: none
    """
    scratch = {
        "H1": "00001",
        "H2": 1,
        "H3": 99,
        "H4": 999
    }

    lv =  {
    "LVID": 12995,
    "PDID": "ERO_KAP_PROVI_1996",
    "GENERATION": 1,
    "MANDANTID": 7002,
    "VERZINSBEGINN": "",
    "ZWEIINK": 12,
    "ZINSBDEP": "",
    "BEARBID": 1,
    "BEARBIDABG": 2,
    "GUEBISZINS": "",
    "BETRAG": 0,
    "WAEHRUNGID": 6,
    "LVBEGT": "1999-12-01",
    "LVABLT": "2046-12-01",
    "TRDK": 0,
    "BTRDIFFMIG": 0,
    "BTRABGL": 0,
    "VSTKK": 0,
    "UEBVERR": 0,
    "LVJAHRTAG": "1999-12-01",
    "LVSTATUS": 7,
    "ZB": 56.93,
    "VZBORIG": 0,
    "KZVORG": "",
    "KZ_ANR": "",
    "ANREWERT": 0,
    "ANNAHMEART": "",
    "FR_SWITCH": "",
    "LET_SWITCH": "",
    "KZ_VERSST": "",
    "VERSST": 0,
    "RENTBEKZ": "",
    "VERTRIEBSWEG": 0,
    "VERSSTNATE": 0,
    "ERRBTGNIV": 0,
    "MINBTG": 0,
    "ZWEIVORZA": 1,
    "BTRDIFFWSW": 0,
    "KZNACHZ": "",
    "UEBVERRNSP": 0,
    "KZANBIETERWECHSEL": "",
    "KZRELEASE": 1821000,
    "ANTRAGSSTEUERUNG": 1,
    "PARTKEY": 2019,
    "KZANGVERS": "",
    "KZAUSZSTOP": "",
    "KZABFINDUNGKBR": 0,
    "KOLLNR": "",
    "LFDKOLLNR": "",
    "VERWGRPNR": "",
    "MUSTERID": "",
    "ZUZAHLPOL": 0,
    "ISVORAUSSSCHAEDVERW": "",
    "KISTAMWIRKDAT": "",
    "KISTAMBEARBDAT": "",
    "KZLEISTUNGSARTTOD": 0,
    "STEULANDSEKUNDAER": "",
    "STEUZUSCHLAGZUZAHL": "",
    "STEUZUSCHLAG": 0,
    "STEULANDPRIMAER": 1,
    "KZRECHENKERN": 1,
    "VORGPERFORMANCE": 0,
    "C_WERBHILF": "",
    "C_ZUGWEG": "",
    "C_WIKMGL": ""
}

    bearbnw = {
		"LVID" : "12995",
		"BEARBID" : "10",
		"SCHRITTID" : "201",
		"BEARBEITERID" : "Batch",
		"BEARBDAT" : "2019-12-10T22:00:00Z",
		"WIRKBEGINN" : "2019-02-28T22:00:00Z",
		"FEINBEGINN" : "3",
		"WIRKENDE" : "2019-03-31T21:00:00Z",
		"FEINENDE" : "-3",
		"BEARBIDSTO" : "0",
		"BEARBDATSTO" : "",
		"JOBID" : "lifecon\/per\/20191211-154214377",
		"BEARBGRDID" : "0",
		"MELDEDAT" : "",
		"WTERM" : "",
		"BEARBZEIT" : "15:44:41",
		"SCHWEBEID_4AUGEN" : "",
		"BEARBEITERID_4A" : "",
		"PARTKEY" : "2019",
		"BUSINESSPROCESSID" : "32758",
		"KBFAUFTRAGSID" : "",
		"BEARBKATEGORIE" : "T",
		"KZILISRELEVANT" : "0",
		"BEARBRESULT" : "",
		"BEARBIDABG" : "11"    
}
    sample_records = {"bearbnw.csv": bearbnw, "lv.csv": lv, "scratch.csv": scratch}

    record = sample_records[file_name]
    qual_file_name = path + '/' + file_name

    if os.path.exists(qual_file_name):
        os.remove(qual_file_name)

    f = open(qual_file_name, "at")
    column_header = ",".join(('"' + b + '"' for b in record.keys()))
    column_header = column_header.replace('"', '').replace("'", "")
    f.write(column_header + '\n')

    if not rows or not header_lst:
        new_record_str = ",".join(('"' + x + '"' if isinstance(x, str) else x for x in record.values()))
        new_record_str = new_record_str.replace('"', '').replace("'", "")
        f.write(new_record_str + '\n')
    else:
        for row in rows:
            record_line = copy.deepcopy(record)
            for hdr in header_lst:
                record_line[hdr] = row[hdr]
            new_record_str = ",".join((x if isinstance(x, str) else str(x) for x in record_line.values()))
            f.write(new_record_str + '\n')
    f.close()


@step("the sql statement on join returns")
def step_validate_query(context):
    query = "select count(*) counter, b.bearbidabg b_bearbidabg, l.bearbidabg l_bearbidabg from core_bddtest.bearbnw b join core_bddtest.lv l on b.bearbnw_id = l.bearbnw_id group by b.bearbidabg,l.bearbidabg order by b.bearbidabg asc"
    expected =[]
    for heading in context.table.headings:
        expected.append([(int(row[heading])) if row[heading].isdigit() else row[heading] for row in context.table])
    df = context.spark.sql(query)
    pdf = df.toPandas()
    actual = [list(pdf[x]) for x in pdf.columns]
    np.testing.assert_array_equal(actual, expected)

@step("the following scripts are executed")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    os.chdir('../src/remote/python')
    heading = context.table.headings[0]
    for row in context.table:
        run_cmd(['spark-submit ', '--properties-file conf/spark.conf --jars db2jcc4.jar' + row[heading]])