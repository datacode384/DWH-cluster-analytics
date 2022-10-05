# Performance tests 

1. Ingest varying message size and number of messages through https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/behave-testing/framework_ib_bem.feature to IBM MQ Explorer of below credentials 
```
Input queue: L0.DWH.IN.1 
Bad records queue: L0.DWH.BACKOUT.1 (backout queue for L0.DWS.IN.1 with threshold = 1)
You can connect with MQ Explorer using the following coordinates:
Host: 10.194.5.252
Port: 32090
Queue Manager: QMPUBLIC
user/psw: app/ZrOp73QqAa3
```
2. Run the scala script 

  - cd /performace_tests_msg_size_varies/intellij_projetcs/
  - Remove previous compilation generated folders like project / target folders and recompile as below. these folders will be generated upon below 2 steps. sbt version in project/build.properties set to latest 1.3.8
  - sbt compile 
  - sbt run

3. 4 Test scenraio times when there is no load / no other processes running on 10.85.52.14 machine are shown below
https://dth03.ibmgcloud.net/confluence/pages/viewpage.action?spaceKey=DWHIBM&title=Performancetest#

4. Note: Spark streaming program runs continously untill user sends a interrupt signal manually like ctrl + c or when os kills the program with sigkill. Right now, sigkill cant be caught in program with exception handling as it is os generated hardware resource error and not originated from program. sigkill is issued by os when vm is running out of space or when many processes are running on vm and space cant be allocated to our process. So, when ever our spark program stops, its either graceful interruption from user with ctrl+c or forceful interruption from os with sigkill or other os signals. In other cases of software related exceptions, these can be caught and printed to user.  
