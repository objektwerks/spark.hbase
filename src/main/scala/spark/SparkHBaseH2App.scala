package spark

import java.sql.{Connection, DriverManager, Statement}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import play.api.libs.json.Json

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object SparkHBaseH2App extends App {
  val log = Logger.getLogger(getClass.getName)
  val conf = ConfigFactory.load("app.conf")
  H2Proxy(conf)
  val hbaseProxy = HBaseProxy(conf)
  log.info(s"*** Created HBaseProxy and H2Proxy.")
  hbaseProxy.getValues match {
    case Right(values) => runJob(conf, values)
    case Left(throwable) => log.error("*** SparkHBaseH2App failed!", throwable)
  }
  hbaseProxy.close()
  log.info(s"*** Closed HBaseProxy.")

  def runJob(conf: Config, values: Seq[String]): Unit = Try {
    val master = conf.getString("spark.master")
    val app = conf.getString("spark.app")
    val sparkSession = SparkSession.builder.master(master).appName(app).getOrCreate()
    log.info(s"*** Created Spark session.")

    Class.forName(conf.getString("h2.driver"))
    val url = conf.getString("h2.url")
    val user = conf.getString("h2.user")
    val password = conf.getString("h2.password")
    log.info(s"*** Loaded JDBC driver.")

    import sparkSession.implicits._

    val dataset = sparkSession.createDataset(values)
    dataset.foreach { value =>
      var connection: Connection = null
      var statement: Statement = null
      try {
        val keyValue = Json.parse(value).as[KeyValue]
        connection = DriverManager.getConnection(url, user, password)
        statement = connection.createStatement()
        val result = statement.executeUpdate(s"insert into kv values(${keyValue.key}, ${keyValue.value})")
        log.info(s"*** JDBC insert: $value with result: $result")
      } catch { case NonFatal(e) =>
        log.error(s"JDBC error.", e)
      } finally {
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      }
    }

    sparkSession.close()
    log.info(s"*** Closed Spark session.")
  } match {
    case Success(_) => log.info("*** SparkHBaseH2App succeeded!")
    case Failure(throwable) => log.error("*** SparkHBaseH2App failed!", throwable)
  }
}