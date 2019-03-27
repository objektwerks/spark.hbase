name := "spark.hbase"
organization := "objektwerks"
version := "0.1"
scalaVersion := "2.12.8"
libraryDependencies ++= {
  val sparkVersion = "2.4.0"
  val hbaseVersion = "2.1.4"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.hbase" % "hbase-client" % hbaseVersion,
    "org.apache.hbase" % "hbase-server" % hbaseVersion,
    "org.apache.hbase" % "hbase-common" % hbaseVersion,
    "org.apache.hadoop" % "hadoop-core" % "1.2.1",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.5",
    "com.typesafe.play" %% "play-json" % "2.7.2",
    "org.scalikejdbc" %% "scalikejdbc" % "3.3.3",
    "com.h2database" % "h2" % "1.4.197",
    "com.typesafe" % "config" % "1.3.3",
    "org.scalatest" %% "scalatest" % "3.0.5" % Test
  )
}