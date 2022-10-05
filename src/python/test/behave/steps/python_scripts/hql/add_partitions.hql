--For reference:
--https://docs.cloudera.com/documentation/enterprise/5-8-x/topics/impala_alter_table.html

--Check to see the unique elements from the partitioned columns of the table:
--SELECT DISTINCT process_id, tenant_name FROM core.lv_test8;

--Check the existing partitions on desired table:
--SHOW PARTITIONS core.lv_test8;

--Delete a partition:
--ALTER TABLE core.lv_test8 DROP IF EXISTS PARTITION (process_id=87,tenant_name="ergo");

--Create a new partition:
--ALTER TABLE core.lv_test8 ADD IF NOT EXISTS PARTITION (process_id=87,tenant_name="ergo");
