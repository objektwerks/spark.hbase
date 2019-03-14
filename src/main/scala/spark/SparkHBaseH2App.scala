package spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object SparkHBaseH2App {
  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName)
    val conf = ConfigFactory.load("app.conf")
    val h2Proxy = H2Proxy(conf)
    val hbaseProxy = HBaseProxy(conf)
    log.info(s"*** Created HBaseProxy and H2Proxy.")
    hbaseProxy.getRowKeys match {
      case Right(rowKeys) => runJob(log, conf, hbaseProxy, h2Proxy, rowKeys)
      case Left(throwable) => exit(log, throwable)
    }
  }

  def runJob(log: Logger, conf: Config, hbaseProxy: HBaseProxy, h2Proxy: H2Proxy, rowKeys: Seq[String]): Unit = Try {
    val master = conf.getString("spark.master")
    val app = conf.getString("spark.app")
    val sparkSession = SparkSession.builder.master(master).appName(app).getOrCreate()
    log.info(s"*** Created Spark session.")

    sys.addShutdownHook {
      hbaseProxy.close()
      h2Proxy.close()
      sparkSession.stop()
      log.info(s"*** Closed HBaseProxy and H2Proxy. Stopped Spark session.")
    }

    import sparkSession.implicits._

    val dataset = sparkSession.createDataset(rowKeys)
    dataset.foreach { rowKey =>
      hbaseProxy.getValueByRowKey(rowKey) match {
        case Right(value) => h2Proxy.insert(rowKey, value)
        case Left(throwable) => log.error(s"HBaseProxy.getValueByRowKey($rowKey) failed!", throwable)
      }
    }
  } match {
    case Success(_) => exit(log, h2Proxy)
    case Failure(throwable) => exit(log, throwable)
  }

  def exit(log: Logger, h2Proxy: H2Proxy): Unit = {
    log.info(s"*** H2Proxy list: ${h2Proxy.list}")
    log.info("*** SparkHBaseH2App succeeded!")
    System.exit(0)
  }

  def exit(log: Logger, throwable: Throwable): Unit = {
    log.error("*** SparkHBaseH2App failed!", throwable)
    System.exit(-1)
  }
}