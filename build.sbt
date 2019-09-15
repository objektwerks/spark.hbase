name := "spark.hbase"
organization := "objektwerks"
version := "0.1"
scalaVersion := "2.12.10"
libraryDependencies ++= {
  val sparkVersion = "2.4.4"
  val hbaseVersion = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.hbase" % "hbase-client" % hbaseVersion,
    "org.apache.hbase" % "hbase-server" % hbaseVersion,
    "org.apache.hbase" % "hbase-common" % hbaseVersion,
    "org.apache.hadoop" % "hadoop-core" % "1.2.1",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9",
    "com.typesafe.play" %% "play-json" % "2.7.4",
    "org.scalikejdbc" %% "scalikejdbc" % "3.3.5",
    "com.h2database" % "h2" % "1.4.197",
    "com.typesafe" % "config" % "1.3.4",
    "org.scalatest" %% "scalatest" % "3.0.8" % Test
  )
}