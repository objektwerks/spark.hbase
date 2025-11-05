name := "spark.hbase"
organization := "objektwerks"
version := "0.1"
scalaVersion := "2.12.20"
libraryDependencies ++= {
  val sparkVersion = "2.4.8"
  val hbaseVersion = "2.6.3"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.hbase" % "hbase-client" % hbaseVersion,
    "org.apache.hbase" % "hbase-server" % hbaseVersion,
    "org.apache.hbase" % "hbase-common" % hbaseVersion,
    "org.apache.hadoop" % "hadoop-core" % "1.2.1",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.3",
    "com.typesafe.play" %% "play-json" % "2.10.4",
    "org.scalikejdbc" %% "scalikejdbc" % "4.3.2",
    "com.h2database" % "h2" % "2.4.240",
    "com.typesafe" % "config" % "1.4.3"
  )
}
