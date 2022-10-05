# Config setup
- In Dev, link ```ln -sf dev.json config.json```. In Cit, link ```ln -sf cit.json config.json```

- simple run 
    - ```python spark_copy_hive_tbl_data.py```
    - ```python spark_count_words.py```

- spark-submit specific 
    -  Master local in client mode  
        - ```spark-submit spark_copy_hive_tbl_data.py``` (or) ```spark-submit --master local --deploy-mode client *.py``` 
    - Install requiremnts in RHEL cluster with multiple nodes to run with --master yarn
        - Install venv in /usr/local location. The dafault permissions of /home/ergo*/ location are not supported to install venv in user specifc directory to run with --master yarn. 
        - Install same venv and actiavte venv in all nodes
        - Install all the required python libraries in all the nodes from requirements.txt to run with master yarn
    - Master yarn with client mode example command
        - ```spark-submit --master yarn --deploy-mode client *.py```
    - Mater yarn with cluster mode example command
         - ```spark-submit --master yarn --deploy-mode cluster  --files config.json *.py``` 

- Note: yarn log generated with ```yarn logs -applicationId #application-id > log.txt ```

# SIT Specific smoke test run instructions

- As SIT installation has kerberos authentication enabled , the run instructions differ as below for SIT 

- link ```ln -sf cit.json config.json```

- For running scripts in Pyspark folder with python / spark-submit in client/cluster modes, 
    - Before running hive related jobs like spark_count_words_2ndIteration.py, initiate hive kerberos principal with ```knit hive```and enter pwd: ```ITERGOpw4hdfsprincipal```
    - Before running hdfs related jobs like spark_copy_hive_tbl_data_2ndIteration.py, initiate hive kerberos principal with ```knit hdfs``` and enter pwd: ```ITERGOpw4hdfsprincipal```. Copy the file to [/user/hdfs location](/deployment/smoke-tests-package/python/pyspark/copy-file-to-hdfs.sh#L2) as other users dont have permissions to access hdfs files 
