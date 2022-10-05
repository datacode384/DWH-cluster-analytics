/bin/beeline -u "jdbc:hive2://machine:port/" -n admin -p admin -f pas_lv_raw_zone_test.hql
/bin/beeline -u "jdbc:hive2://machine:port/" -n admin -p admin -f pas_lv_raw_zone_test1.hql
/bin/beeline -u "jdbc:hive2://machine:port/" -n admin - p admin -f insert.hql