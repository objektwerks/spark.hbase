name := "spark.hbase"
organization := "objektwerks"
version := "0.1"
scalaVersion := "2.12.12"
libraryDependencies ++= {
  val sparkVersion = "2.4.7"
  val hbaseVersion = "2.3.2"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.hbase" % "hbase-client" % hbaseVersion,
    "org.apache.hbase" % "hbase-server" % hbaseVersion,
    "org.apache.hbase" % "hbase-common" % hbaseVersion,
    "org.apache.hadoop" % "hadoop-core" % "1.2.1",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.3",
    "com.typesafe.play" %% "play-json" % "2.9.1",
    "org.scalikejdbc" %% "scalikejdbc" % "3.5.0",
    "com.h2database" % "h2" % "1.4.200",
    "com.typesafe" % "config" % "1.4.0"
  )
}
