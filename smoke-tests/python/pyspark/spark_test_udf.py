
from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, array_contains, array, array_except, explode, json_tuple
from datetime import datetime

data = json.load(open("config.json","r"))

key_obj = {"application_name": "DWH",
                "source_database" : "raw_zone",
                "source_table" : "bpms_bem_l0_dwh_in_1",
                "target_database" : "core",
                "target_table" : "bpms_bem_e2e_process_timeslot",
                "target_key_table" : "key_bpms_bem_e2e_process_timeslot",
                "tenant_name": "",
                "source_system" : "201",
                "join_tables" : [],
                "message_filter" : "{'age'}",
                "primaryKeys": { "id":{"field_name":"bpms_bem_e2e_process_timeslot_id","fields":["e2e_process_timeslot_id"]}, "sid":{"field_name":"bpms_bem_e2e_process_timeslot_sid","fields":["e2e_process_timeslot_id","jms_message_id","process_id"]} },
                "foreignKeys": { "bpms_bem_business_event_id":["business_event_id"],"bpms_bem_e2e_process_id":["e2e_process_id"],"bpms_bem_package_entry_id":["external_correlation_id","package_entry_id"] }
        }

for x in data["spark"]:
        spark = SparkSession.builder.config(x,data["spark"][x]).enableHiveSupport().getOrCreate()

def check_key_obj(key_obj, message):
        key_obj = set( eval(key_obj["message_filter"]))
        if type(message) == list:
                return key_obj.issubset(set(map(lambda x: camel_to_snake(x).upper(),functools.reduce(lambda a,b: set(a).union(b), message))))
        elif type(message) == dict:
                return key_obj.issubset(set(map(lambda x: camel_to_snake(x).upper(), message)))
        else:
                return False

df=spark.createDataFrame([('key1','{ "name":"John", "age":30, "city":"New York"}')],['key','value'])
df=df.union(spark.createDataFrame([('key2','{ "name":"John", "age":30, "city2":"New York"}')],['key','value']))
df=df.union(spark.createDataFrame([('key3','{ "name":"John", "age":31, "city":"New York"}'),('key4','{ "name":"John", "age":34, "city":"New York"}')],['key','value']))


@udf(returnType=ArrayType(StringType()))
def get_cols(json_string):
    t=json.loads(json_string)
    c=list(set(t.keys()))
    return c

df=df.withColumn('keys', get_cols(df.value))

parse_dicts = udf(lambda x: json.loads(x), MapType(StringType(), StringType()))
parse_lists = udf(lambda x: json.loads(x) if x[0]=='[' else None, ArrayType(MapType(StringType(), StringType())))
check_obj   = udf(lambda x: check_key_obj(key_obj,x), BooleanType())

df = df.withColumn("dict_msg", parse_dicts("value"))

df.show()
df = df.withColumn("list_msg", parse_lists("value"))
df.show()
df= df.withColumn("dict_filter", check_obj("value"))
df.show()

def get_datetime_now():
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("Current Time =", current_time)
get_datetime_now()
df.count()
