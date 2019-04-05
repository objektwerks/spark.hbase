package spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import play.api.libs.json.Json

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object SparkHBaseH2App extends App {
  val log = Logger.getLogger(getClass.getName)
  val conf = ConfigFactory.load("app.conf")

  H2Proxy(conf.getConfig("h2")).init()
  val hbaseProxy = HBaseProxy(conf.getConfig("hbase"))
  hbaseProxy.getRowKeys map { rowKeys =>
    runJob(conf, rowKeys)
  } recover {
    case NonFatal(e) => log.error("*** SparkHBaseH2App: HbaseProxy failed!", e)
  }

  def runJob(conf: Config, rowKeys: Seq[String]): Unit = Try {
    val master = conf.getString("spark.master")
    val app = conf.getString("spark.app")
    val sparkSession = SparkSession.builder.master(master).appName(app).getOrCreate()
    log.info("*** SparkHBaseH2App: Created Spark session.")

    import sparkSession.implicits._

    val hbaseProxy = HBaseProxy(conf.getConfig("hbase"))
    val h2Proxy = H2Proxy(conf.getConfig("h2"))

    sparkSession.createDataset(rowKeys).foreach { rowKey =>
      hbaseProxy.getValueByRowKey(rowKey) match {
        case Success(value) =>
          val keyValue = Json.parse(value).as[KeyValue]
          val result = h2Proxy.insert(keyValue)
          assert(result == 1)
        case Failure(e) => log.error(s"*** SparkHBaseH2App: Processing $rowKey failed!", e)
      }
    }

    sparkSession.close()
    log.info(s"*** SparkHBaseH2App: Closed Spark session.")
  } match {
    case Success(_) => log.info("*** SparkHBaseH2App: Job succeeded!")
    case Failure(e) => log.error("*** SparkHBaseH2App: Job failed!", e)
  }
}