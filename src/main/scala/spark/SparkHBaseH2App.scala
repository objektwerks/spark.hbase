package spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import play.api.libs.json.Json

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object SparkHBaseH2App extends App {
  val log = Logger.getLogger(getClass.getName)
  val conf = ConfigFactory.load("app.conf")
  val h2Proxy = H2Proxy(conf)
  val hbaseProxy = HBaseProxy(conf)
  log.info(s"*** Created HBaseProxy and H2Proxy.")
  hbaseProxy.getValues match {
    case Right(values) => runJob(values)
    case Left(throwable) => log.error("*** SparkHBaseH2App failed!", throwable)
  }
  hbaseProxy.close()
  log.info(s"*** Closed HBaseProxy.")

  def runJob(values: Seq[String]): Unit = Try {
    val master = conf.getString("spark.master")
    val app = conf.getString("spark.app")
    val sparkSession = SparkSession.builder.master(master).appName(app).getOrCreate()
    log.info(s"*** Created Spark session.")

    import sparkSession.implicits._

    val dataset = sparkSession.createDataset(values)
    dataset.foreach { value =>
      try {
        val keyValue = Json.parse(value).as[KeyValue]
        val result = h2Proxy.executeUpdate(s"insert into kv values(${keyValue.key}, ${keyValue.value})")
        assert(result == 1)
      } catch {
        case NonFatal(e) => log.error(s"JDBC error.", e)
      }
    }

    sparkSession.close()
    log.info(s"*** Closed Spark session.")
  } match {
    case Success(_) => log.info("*** SparkHBaseH2App succeeded!")
    case Failure(throwable) => log.error("*** SparkHBaseH2App failed!", throwable)
  }
}