package com.ibm.spark.streaming.mq

import org.apache.log4j.{Logger, Level}

import java.util.{Calendar, Properties, UUID}

import java.net.ServerSocket
import javax.naming.{Context, InitialContext}
import javax.jms._

import net.tbfe.spark.streaming.jms.JmsStreamUtils

import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType, StructField}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Milliseconds, Minutes}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.{HiveSessionStateBuilder, HiveContext}

import org.apache.spark.sql.hive.HiveExternalCatalog

import scala.concurrent.duration._

object SparkMQExample
{

  // functionToCreateContext” provides all the information on
  // how to start the Streaming Context and what operations to be performed on the incoming data.
  // All this information is saved in the checkpoint directory in case of a failure.
  // To enable this checkpointing, “writeAheadLog” must be enabled and the checkpointing directory to
  // be passed to the Context’s “checkpoint” method.
  def functionToCreateContext(host: String,
                               port: String,
                               qm: String,
                               qn: String,
                               qc: String,
                               checkpointDirectory: String
                             ): StreamingContext = {


    val sparkConf = new SparkConf().
      setAppName("MQJMSReceiver").
      //set("spark.streaming.receiver.writeAheadLog.enable", "true").
      //.set("spark.driver.allowMultipleContexts", "true").
      set("spark.debug.maxToStringFields", "10000").
      set("spark.hadoop.hive.exec.dynamic.partition.mode", "non-strict").
      set("hive.exec.dynamic.partition.mode", "nonstrict").
      set("spark.hadoop.hive.exec.dynamic.partition", "true").
      set("hive.exec.dynamic.partition", "true").
      set("spark.jars.packages", "com.databricks:spark-xml_2.11:0.5.0").
      set("hive.metastore.uris","thrift://10.85.52.12:9083").
      set("spark.sql.warehouse.dir","hdfs://10.85.52.12:8020/user/hive/warehouse").
      set("hive.metastore.warehouse.dir", "hdfs://10.85.52.12:8020/user/hive/warehouse").
      set("spark.sql.catalogImplementation", "hive").
      set("spark.sql.hive.metastore.sharedPrefixes","org.postgresql").
      set("spark.hadoop.javax.jdo.option.ConnectionURL","jdbc:postgresql://10.85.93.209:5432/hive").
      set("spark.hadoop.javax.jdo.option.ConnectionDriverName","org.postgresql.Driver").
      set("spark.hadoop.javax.jdo.option.ConnectionUserName","hive").
      set("spark.hadoop.javax.jdo.option.ConnectionPassword","hive").
      setMaster("local[*]").
      set("spark.streaming.stopGracefullyOnShutdown", "true")
      //enableHiveSupport()
      //set("spark.master", "local")
      //set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(10))

    // set checkpoint directory for saving the data to recover when there is crash
    ssc.checkpoint(checkpointDirectory)

    //sc.setLogLevel("ERROR")

    //val hc = new HiveContext(sc)


    // Credentials were used here as we didn't wish to pass as a parameter;
    // passing a file with credentials could be considered also
    //There are many ways that credentials can be passed but for simplicity this has just been hard coded in this example.
    val user = "app"
    val credentials = "ZrOp73QqAa3"
    //val credentials = "kdjUd731"

    //When a message is returned from the service, it is a Message Object.
    // We are converting this to type String to allow for easier processing.

    val converter: Message => Option[String] = {
      case msg: TextMessage =>
        // write these 3 messages to a json object and then write this json object to hive !
        //send this message as a proer json message with 3 fields -
        // {"message":msg.getText, "JMSMessageID":msg.getJMSMessageID, "JMSTimestamp": msg.getJMSTimestamp}
        // Add insert timestamp, date, month and other fields in above json object
        Some(msg.getJMSMessageID + ";" + "L0.DWH.IN.1" +";"+ msg.getText + ";" + msg.getJMSTimestamp + ";" + Calendar.getInstance().getTimeInMillis() + ";" + Calendar.getInstance().get(Calendar.MONTH) + ";" + Calendar.getInstance().get(Calendar.DATE))
      case _ =>
        None
    }
    //We are creating a synchronous JMS queue stream using the utilities file from tbfenet
    // using client acknowledgement to ensure the message is received once and only once.
    // The method to create this stream requires the Spark Streaming Context,
    // and a consumer factory and associated parameters, in our case the MQConsumerFactory.

    val msgs = JmsStreamUtils.createSynchronousJmsQueueStream(
      ssc,
      MQConsumerFactory(
        host,
        port.toInt,
        qm,
        qn,
        user,
        credentials,
        qc
      ),
      converter,
      50,
      1.second,
      10.seconds,
      StorageLevel.MEMORY_AND_DISK_SER_2
    )
    //print("print msgs here", msgs.getClass() )
    //val  start = Calendar.getInstance().getTimeInMillis()
    try {

      msgs.foreachRDD(rdd => {

        // change this to use dstream and get msg to spark df

        if (!rdd.partitions.isEmpty) {
          // This is where you process the messages received

          //println(Console.BLUE + "messages received:")
          println("messages received:")

          rdd.foreach(println) //print("rdd class",rdd.getClass)

          val rddtransform_rows = rdd.map(rdd => rdd.split("\\;"))
            .map(y => Row(y(0).toString, y(1).toString, y(2).toString, y(3).toString, y(4).toString, y(5).toString, y(6).toString))

          //.toDF("jms_message_id", "message_queue", "message", "jms_timestamp", "insert_tst", "insert_month", "insert_day")
          //.show(false)//.take(10)

          rddtransform_rows.take(1).foreach(println)

          //val sqlc = SQLContext.getOrCreate(sc)
          val sqlc = SQLContext.getOrCreate(rdd.context)

	  //val sqlc = new HiveContext(rdd.context)

          val schema = StructType(
            List(
              StructField("jms_message_id" , StringType, true),
              StructField("message_queue", StringType, true),
              StructField("message", StringType, true),
              StructField("jms_timestamp", StringType, true),
              StructField("insert_tst", StringType, true),
              StructField("insert_month", StringType, true),
              StructField("insert_day", StringType, true)

            )
          )

          val df = sqlc.createDataFrame(rddtransform_rows, schema)

          df.show(false)

          df.registerTempTable("temptable")
          //df.createOrReplaceTempView("temptable")

          //sqlc.sql("show databases").show()
          sqlc.sql("insert into raw_zone.bpms_bem_l0_dwh_in_1 select * from temptable ")//bpms_bem_l0_dwh_backout_1
          //Spark.sql("insert into raw_zone.test1 select * from temptable ")
          //rdd.saveAsTextFile("/Users/krishna/Desktop/intellij_projetcs/test/" + Calendar.getInstance().getTimeInMillis())
          //rdd.toDF()

        } else {
          println("rdd is empty")
        }

      })

    }
    catch
      {
        case ex: Exception =>
          ssc.stop(true, true)
          throw ex //exit with error condition
      }

    //Return the streaming context
    ssc
  }


  def main(args: Array[String]): Unit = {


    //val  start = Calendar.getInstance().getTimeInMillis()

    if (args.length < 4) {
      System.err.println("Usage: <host> <port> <queue manager> <queue name> <channel name>")
      //System.exit(1)
    }

    //val Array(host, port, qm, qn) = args

    // Specify the path for the checkpoint directory can be local file or hdfs
    // This is used to recover messages if the application fails

    //Non-reliable local checkpoint directory
    //val checkpointDirectory = "/home/ergo.liferunoffinsuranceplatform.com/kdamarla/DWH-Cluster-Analytics/src/remote/scala/performance_tests_msg_size_varies/intellij_projetcs/checkpoint"
    //val checkpointDirectory = "file:///home/ergo.liferunoffinsuranceplatform.com/kdamarla/lip-core-dwh/src/scala/scripts/mq_read/checkpoint"

    //Reliable hdfs checkpoint directory
    //val checkpointDirectory = "hdfs://10.85.52.12:8020/checkpoint3/"//+ Calendar.getInstance().getTimeInMillis()

    val checkpointDirectory = "./checkpoint"//hdfs://10.85.52.12:8020/checkpoint/"
    /*
    val conf = new SparkConf().setAppName("lamba spark")//.setMaster("local[*]")

    val config = new SparkConf().
      set("spark.debug.maxToStringFields", "10000").
      set("spark.hadoop.hive.exec.dynamic.partition.mode", "non-strict").
      set("hive.exec.dynamic.partition.mode", "nonstrict").
      set("spark.hadoop.hive.exec.dynamic.partition", "true").
      set("hive.exec.dynamic.partition", "true").
      set("spark.jars.packages", "com.databricks:spark-xml_2.11:0.5.0").
      set("spark.sql.warehouse.dir","hdfs://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:8020/user/hive/warehouse").
      set("hive.metastore.warehouse.dir", "hdfs://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:8020/user/hive/warehouse").
      set("hive.metastore.uris","thrift://dwh-hdp-node01.dev.ergo.liferunoffinsuranceplatform.com:9083").
      set("spark.sql.catalogImplementation", "hive").
      //set("spark.master", "local").
      set("spark.driver.allowMultipleContexts", "true")
    //setMaster("local[*]")

    val Spark1 =  SparkSession.builder().
      config(config).
      enableHiveSupport().
      getOrCreate()
  */
    // Get StreamingContext from checkpoint data or create a new one
    val streamC = StreamingContext.getOrCreate(
      checkpointDirectory,
      () =>
        //functionToCreateContext("10.194.5.252", "32020", "QM1", "L0.DWH.IN.1", checkpointDirectory)
          functionToCreateContext("10.85.197.183", "32090", "QMPUBLIC", "L0.DWH.IN.1", "DEV.APP.SVRCONN", checkpointDirectory)
    )

    try {
      streamC.start()
    }

    catch{
        case e: Exception => streamC.stop(stopSparkContext = true, stopGracefully = true)
        throw e // exit with error condition
    }

    try{
      streamC.awaitTermination()
    }
    //  the session remains active until the user sends a termination signal (e.g. CTRL+C)
    catch
      {
        case e: Exception => streamC.stop(stopSparkContext = true, stopGracefully = true)
        throw e // exit with error condition
      }

    /*
    val end = Calendar.getInstance().getTimeInMillis()
    val time_diff = end - start
    print("time taken to read messages from mq to spark program",time_diff)
    */

  }
}



