package spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SparkHBaseApp extends App {
  val log = Logger.getLogger(getClass.getName)
  val conf = ConfigFactory.load("app.conf")

  val hbaseProxy = HBaseProxy(conf)
  val hbaseValues = hbaseProxy.values()

  val master = conf.getString("spark.master")
  val app = conf.getString("spark.app")
  val sparkSession = SparkSession.builder
    .master(master)
    .appName(app)
    .getOrCreate()
  log.info(s"*** Created Spark session for app: $app")

  sys.addShutdownHook {
    sparkSession.stop()
    log.info(s"*** Stopped Spark session for app: $app")
  }

  import sparkSession.implicits._

  val dataset = sparkSession.createDataset(hbaseValues.getOrElse(Seq.empty[String]))
}