package spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object SparkHBaseH2App extends Serializable {
  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName)
    val conf = ConfigFactory.load("app.conf")
    val h2Proxy = H2Proxy(conf)
    val hbaseProxy = HBaseProxy(conf)
    sys.addShutdownHook {
      hbaseProxy.close()
      h2Proxy.close()
      log.info(s"*** Closed HBaseProxy and H2Proxy.")
    }
    log.info(s"*** Created HBaseProxy and H2Proxy.")
    hbaseProxy.getValues match {
      case Right(values) => runJob(log, conf, h2Proxy, values)
      case Left(throwable) => exit(log, throwable)
    }
  }

  def runJob(log: Logger, conf: Config, h2Proxy: H2Proxy, values: Seq[String]): Unit = Try {
    val master = conf.getString("spark.master")
    val app = conf.getString("spark.app")
    val sparkSession = SparkSession.builder.master(master).appName(app).getOrCreate()
    log.info(s"*** Created Spark session.")

    sys.addShutdownHook {
      sparkSession.stop()
      log.info(s"*** Stopped Spark session.")
    }

    import sparkSession.implicits._

    val dataset = sparkSession.createDataset(values)
    dataset.foreach { value =>
      h2Proxy.insert(value, value)
      log.info(s"*** H2Proxy insert: $value")
    }
  } match {
    case Success(_) => exit(log)
    case Failure(throwable) => exit(log, throwable)
  }

  def exit(log: Logger): Unit = {
    log.info("*** SparkHBaseH2App succeeded!")
    System.exit(0)
  }

  def exit(log: Logger, throwable: Throwable): Unit = {
    log.error("*** SparkHBaseH2App failed!", throwable)
    System.exit(-1)
  }
}