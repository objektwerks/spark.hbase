package spark

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

case class SparkProxy(conf: Config) extends Product with Serializable {
  val log = Logger.getLogger(getClass.getName)
  val app = conf.getString("spark.app")
  val sparkSession = SparkSession.builder
    .master(conf.getString("spark.master"))
    .appName(app)
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  log.info(s"*** Created Spark session for app: $app")

  sys.addShutdownHook {
    sparkSession.stop()
    log.info(s"*** Stopped Spark session for app: $app")
  }
}