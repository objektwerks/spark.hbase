package spark

import com.typesafe.config.ConfigFactory

object SparkHBaseApp extends App {
  val conf = ConfigFactory.load("app.conf")
  val hbaseProxy = HBaseProxy(conf)
  val sparkProxy = SparkProxy(conf)
}