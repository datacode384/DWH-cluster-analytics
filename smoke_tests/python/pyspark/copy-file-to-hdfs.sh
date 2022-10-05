echo "copying local file to hdfs"
hdfs dfs -put -f ./shakespeare.txt /user/$USER
echo "changing file permissions to rwx"
sudo -u hdfs hdfs dfs -chmod 777 /user/$USER/shakespeare.txt
sudo -u hdfs hdfs dfs -chown hdfs:supergroup /user/$USER/shakespeare.txt
