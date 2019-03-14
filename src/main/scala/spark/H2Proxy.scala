package spark

import com.typesafe.config.Config
import org.apache.log4j.Logger
import scalikejdbc._

case class H2Proxy(conf: Config) {
  val log = Logger.getLogger(getClass.getName)

  Class.forName(conf.getString("h2.driver"))
  ConnectionPool.singleton(conf.getString("h2.url"), conf.getString("h2.user"), conf.getString("h2.password"))
  log.info(s"*** Connected to H2 datasource: ${conf.getString("h2.url")}")

  implicit val session = AutoSession
  sql"""
        drop table kv if exists;
        create table kv (key varchar(64) not null, value varchar(64) not null);
    """.execute.apply
  log.info("*** Executed H2 DDL to drop and create kv table.")

  def insert(key: String, value: String): Unit = {
    sql"insert into kv values ($key, $value)".update.apply
    log.info(s"*** Executed H2 insert into kv values ($key, $value)")
    ()
  }

  def list: List[(String, String)] = {
    val list = sql"select * from kv".map(rs => ( rs.string("key"), rs.string("value") )).list.apply
    log.info("*** Executed H2 select * from kv.")
    list
  }
}