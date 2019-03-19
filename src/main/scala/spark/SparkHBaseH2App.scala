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
  hbaseProxy.getRowKeys map { rowKeys =>
    runJob(rowKeys)
  } recover {
    case NonFatal(e) => log.error("*** SparkHBaseH2App: HbaseProxy failed!", e)
  }
  hbaseProxy.close()

  def runJob(rowKeys: Seq[String]): Unit = Try {
    val sparkSession = SparkSession.builder.master(conf.getString("spark.master")).appName(conf.getString("spark.app")).getOrCreate()
    log.info("*** SparkHBaseH2App: Created Spark session.")

    import sparkSession.implicits._

    sparkSession.createDataset(rowKeys).foreach { rowKey =>
      hbaseProxy.getValueByRowKey(rowKey) match {
        case Success(value) =>
          val keyValue = Json.parse(value).as[KeyValue]
          val result = h2Proxy.executeUpdate(s"insert into kv values(${keyValue.key}, ${keyValue.value})")
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