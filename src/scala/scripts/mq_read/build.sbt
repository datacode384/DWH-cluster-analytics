name := "spark-mq-receiver"
version := "0.0.1"
scalaVersion := "2.11.7"
crossScalaVersions := Seq("2.10.4","2.11.7")
val sparkVer = "2.4"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

//addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.3")

//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

libraryDependencies ++= Seq(
  "javax.jms" % "jms-api" % "1.1-rev-1",
  "org.apache.activemq" % "activemq-core" % "5.7.0" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.4" % "provided",
  "com.ibm.mq" % "com.ibm.mq.allclient" % "9.1.4.0",
  "commons-logging" % "commons-logging" % "1.2",
  //"org.slf4j" % "slf4j-api" % "1.7.5",
  //"org.slf4j" % "slf4j-simple" % "1.7.5",
  //"com.jcabi" % "jcabi-log" % "0.17.3",
  "org.apache.spark" %% "spark-streaming" % "2.4.4",
  "com.google.guava" % "guava" % "14.0.1",
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-hive" % "2.4.4",
  "org.postgresql" % "postgresql" % "42.2.12"
)

//val projectMainClass = "src.test.scala.com.ibm.spark.streaming.mq"
//mainClass in (Compile, run) := Some(projectMainClass)

// Modify this to the location of your MQ Jar files
//unmanagedBase := baseDirectory.value / ".." / "MQJars"

//Assembly jar file settings
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
//assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}_${sparkVersion.value}_${version.value}.jar"
