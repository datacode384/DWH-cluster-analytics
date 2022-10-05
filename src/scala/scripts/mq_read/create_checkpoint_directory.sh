echo "create a checkpoint directory in hdfs"
sudo -u hdfs hdfs dfs -mkdir /checkpoint3
echo "changing file permissions to rwx"
sudo -u hdfs hdfs dfs -chmod 777 /checkpoint3
sudo -u hdfs hdfs dfs -chown hdfs:supergroup /checkpoint3
# sudo -u hdfs hdfs dfs -rm -r /checkpoint2
