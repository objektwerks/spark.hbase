package spark

import com.typesafe.config.Config
import org.apache.log4j.Logger

object H2Proxy {
  def apply(conf: Config): H2Proxy = new H2Proxy(conf)
}

class H2Proxy(conf: Config) extends Serializable {
  val log = Logger.getLogger(getClass.getName)
  val driver = conf.getString("h2.driver")
  val url = conf.getString("h2.url")
  val user = conf.getString("h2.user")
  val password = conf.getString("h2.password")

  import scalikejdbc._
  Class.forName(driver)
  log.info("*** H2Proxy: Loaded driver.")

  ConnectionPool.singleton(url, user, password)
  implicit val session = AutoSession
  sql"""
      drop table kv if exists;
      create table kv (key varchar(64) not null, value varchar(64) not null);
    """.execute.apply
  log.info("*** H2Proxy: Dropped and created kv table.")

  def insert(keyValue: KeyValue): Int = {
    val result =
      sql"""
           insert into kv values(${keyValue.key}, ${keyValue.value})
        """.update.apply
    log.info(s"*** H2Proxy: Inserted key value: $keyValue with result: $result")
    result
  }
}
