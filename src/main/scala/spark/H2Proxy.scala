package spark

import com.typesafe.config.Config
import scalikejdbc._

case class H2Proxy(conf: Config) {
  Class.forName(conf.getString("h2.driver"))
  ConnectionPool.singleton(conf.getString("h2.url"), conf.getString("h2.user"), conf.getString("h2.password"))

  implicit val session = AutoSession
  sql"""
        drop table kv if exists;
        create table kv (key varchar(64) not null, value varchar(64) not null);
    """.execute.apply

  def insert(key: String, value: String): Unit = {
    sql"insert into kv values ($key, $value)".update.apply
    ()
  }

  def close(): Unit = {
    session.close()
    ConnectionPool.close()
  }
}