package spark

import java.sql.{Connection, DriverManager, Statement}

import com.typesafe.config.Config
import org.apache.log4j.Logger

import scala.util.control.NonFatal

object H2Proxy {
  def apply(conf: Config): H2Proxy = new H2Proxy(conf)
}

class H2Proxy(conf: Config) {
  val log = Logger.getLogger(getClass.getName)

  Class.forName(conf.getString("h2.driver"))
  val url = conf.getString("h2.url")
  val user = conf.getString("h2.user")
  val password = conf.getString("h2.password")
  log.info(s"*** H2 proxy driver loaded.")

  executeUpdate("drop table kv if exists;")
  executeUpdate("create table kv (key varchar(64) not null, value varchar(64) not null);")

  def executeUpdate(sql: String): Unit = {
    var connection: Connection = null
    var statement: Statement = null
    try {
      connection = DriverManager.getConnection(url, user, password)
      statement = connection.createStatement()
      val result = statement.executeUpdate(sql)
      log.info(s"*** H2 proxy executed: $sql with result: $result")
    } catch { case NonFatal(e) => log.error(s"H2 proxy error.", e)
    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }
}
