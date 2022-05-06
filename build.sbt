name := "spark.hbase"
organization := "objektwerks"
version := "0.1"
scalaVersion := "2.12.15"
libraryDependencies ++= {
  val sparkVersion = "2.4.8"
  val hbaseVersion = "2.4.8"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.hbase" % "hbase-client" % hbaseVersion,
    "org.apache.hbase" % "hbase-server" % hbaseVersion,
    "org.apache.hbase" % "hbase-common" % hbaseVersion,
    "org.apache.hadoop" % "hadoop-core" % "1.2.1",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.2",
    "com.typesafe.play" %% "play-json" % "2.9.2",
    "org.scalikejdbc" %% "scalikejdbc" % "3.5.0", // Can't upgrade to 4.0.0
    "com.h2database" % "h2" % "2.1.212",
    "com.typesafe" % "config" % "1.4.2"
  )
}
