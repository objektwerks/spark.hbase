package spark

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.sql.SparkSession

object SparkHBaseApp extends App {
  val appConf = ConfigFactory.load("app.conf").getConfig("app")
  val hbaseConf = HBaseConfiguration.create
  val connection = ConnectionFactory.createConnection(hbaseConf)
  val admin =  connection.getAdmin

  val sparkSession = SparkSession
    .builder
    .master(appConf.getString("master"))
    .appName(appConf.getString("name"))
    .getOrCreate()

  sys.addShutdownHook {
    sparkSession.stop
  }
}