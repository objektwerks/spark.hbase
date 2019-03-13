package spark

import com.typesafe.config.Config
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.Try

class HBaseProxy(conf: Config) {
  val log = Logger.getLogger(getClass.getName)
  val hbaseConf = HBaseConfiguration.create
  val connection = ConnectionFactory.createConnection(hbaseConf)
  val admin =  connection.getAdmin
  val tableName = conf.getString("hbase.table-name")
  val columnFamily = conf.getString("hbase.column-family")
  val putCount = conf.getInt("hbase.put-count")

  def createTable(): Try[Unit] = Try {
    val table = TableName.valueOf(tableName)
    val column = ColumnFamilyDescriptorBuilder.of(columnFamily)
    val descripter = TableDescriptorBuilder.newBuilder(table).setColumnFamily(column).build()
    admin.createTable(descripter)
    log.info(s"*** Created table: $tableName")
  }

  def put(): Try[Unit] = Try {
    val puts = ArrayBuffer.empty[Put]
    val family = columnFamily.getBytes
    for (i <- 1 to putCount) {
      val rowKey = Bytes.toBytes(i)
      val put = new Put(rowKey)
      val qualifier = Bytes.toBytes(i)
      val value = Bytes.toBytes(i + i)
      put.addColumn(family, qualifier, value)
      puts += put
    }
    val table = connection.getTable(TableName.valueOf(tableName))
    table.put(puts.asJava)
    log.info(s"*** Put $putCount rows to table: $tableName")
  }
}

object HBaseProxy {
  def apply(conf: Config): HBaseProxy = new HBaseProxy(conf)
}