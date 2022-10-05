#!/bin/bash
rm -f tableNames.txt
rm -f raw_zone.hql
hive -e "use $1; show tables;" > tableNames.txt
wait
cat tableNames.txt |while read LINE
   do
   hive -e "use $1;show create table $LINE;" >>raw_zone.hql
   echo -e ";" >> raw_zone.hql
   echo  -e "\n" >> raw_zone.hql
   done
rm -f tableNames.txt
echo "Table DDL generated"
