package spark

import com.typesafe.config.Config
import org.apache.log4j.Logger
import scalikejdbc.{AutoSession, ConnectionPool, DB, scalikejdbcSQLInterpolationImplicitDef}

object H2Proxy {
  @transient lazy val log = Logger.getLogger(getClass.getName)
  @transient lazy implicit val session = AutoSession

  def apply(conf: Config): H2Proxy = new H2Proxy(conf)
}

class H2Proxy(conf: Config) extends Serializable {
  import H2Proxy._

  val driver = conf.getString("driver")
  val url = conf.getString("url")
  val user = conf.getString("user")
  val password = conf.getString("password")

  Class.forName(driver)
  ConnectionPool.singleton(url, user, password)
  log.info("*** H2Proxy: Loaded driver and created connection.")

  def init(): Unit = {
    DB localTx { implicit session =>
      sql"""
        drop table kv if exists;
        create table kv (key varchar(64) not null, value int not null);
      """.execute.apply
    }
    session.close()
    log.info(s"*** H2Proxy: Created kv table.")
  }

  def insert(keyValue: KeyValue): Int = {
    val result = DB localTx { implicit session =>
      sql"insert into kv values(${keyValue.key}, ${keyValue.value})".update.apply
    }
    session.close()
    log.info(s"*** H2Proxy: Inserted key value: $keyValue with result: $result")
    result
  }

  def update(keyValue: KeyValue): Int = {
    val result = DB localTx { implicit session =>
      sql"update kv set value = ${keyValue.value} where key = ${keyValue.key}".update.apply
    }
    session.close()
    log.info(s"*** H2Proxy: Updated key value: $keyValue with result: $result")
    result
  }
}