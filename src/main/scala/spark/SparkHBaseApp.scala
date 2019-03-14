package spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object SparkHBaseApp {
  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName)
    val conf = ConfigFactory.load("app.conf")
    runHBaseJob(log, conf)
  }

  def runHBaseJob(log: Logger, conf: Config): Unit = {
    val hbaseProxy = HBaseProxy(conf)
    hbaseProxy.getRowKeys match {
      case Right(rowKeys) => runSparkJob(log, conf, hbaseProxy, rowKeys)
      case Left(throwable) => exit(log, throwable)
    }
  }

  def runSparkJob(log: Logger, conf: Config, hbaseProxy: HBaseProxy, rowKeys: Seq[String]): Unit = Try {
    val master = conf.getString("spark.master")
    val app = conf.getString("spark.app")
    val sparkSession = SparkSession.builder.master(master).appName(app).getOrCreate()
    log.info(s"*** Created Spark session for app: $app")

    sys.addShutdownHook {
      hbaseProxy.close()
      sparkSession.stop()
      log.info(s"*** Stopped Spark session for app: $app")
    }

    import sparkSession.implicits._

    val dataset = sparkSession.createDataset(rowKeys)
    dataset.foreach { rowKey =>
      println(hbaseProxy.getValueByRowKey(rowKey))
    }
  } match {
    case Success(_) => exit(log)
    case Failure(throwable) => exit(log, throwable)
  }

  def exit(log: Logger): Unit = {
    log.info("*** SparkHbase app succeeded!")
    System.exit(0)
  }

  def exit(log: Logger, throwable: Throwable): Unit = {
    log.error("*** SparkHbase app failed!", throwable)
    System.exit(-1)
  }
}