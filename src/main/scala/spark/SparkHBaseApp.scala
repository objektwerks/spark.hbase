package spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object SparkHBaseApp extends App {
  val conf = ConfigFactory.load("app.conf")
  val hbaseProxy = HBaseProxy(conf)

  val sparkSession = SparkSession
    .builder
    .master(conf.getString("master"))
    .appName(conf.getString("name"))
    .getOrCreate()

  sys.addShutdownHook {
    sparkSession.stop
  }
}