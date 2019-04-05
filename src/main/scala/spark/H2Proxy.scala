package spark

import com.typesafe.config.Config
import org.apache.log4j.Logger
import scalikejdbc.{AutoSession, ConnectionPool}
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef

object H2Proxy {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def apply(conf: Config): H2Proxy = new H2Proxy(conf)
}

class H2Proxy(conf: Config) extends Serializable {
  import H2Proxy.log

  val driver = conf.getString("driver")
  val url = conf.getString("url")
  val user = conf.getString("user")
  val password = conf.getString("password")

  Class.forName(driver)
  ConnectionPool.singleton(url, user, password)
  log.info("*** H2Proxy: Loaded driver and created connection.")

  def init(): Unit = {
    implicit val session = AutoSession
    sql"""
      drop table kv if exists;
      create table kv (key varchar(64) not null, value varchar(64) not null);
    """.execute.apply
    session.close()
    log.info(s"*** H2Proxy: Created kv table.")
  }

  def insert(keyValue: KeyValue): Int = {
    implicit val session = AutoSession
    val result =
      sql"""
           insert into kv values(${keyValue.key}, ${keyValue.value})
        """.update.apply
    session.close()
    log.info(s"*** H2Proxy: Inserted key value: $keyValue with result: $result")
    result
  }
}