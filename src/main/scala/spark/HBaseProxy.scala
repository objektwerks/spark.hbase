package spark

import com.typesafe.config.Config
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{FilterList, FirstKeyOnlyFilter, KeyOnlyFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.Try

case class HBaseProxy(conf: Config) {
  val log = Logger.getLogger(getClass.getName)
  val hbaseConf = HBaseConfiguration.create
  val connection = ConnectionFactory.createConnection(hbaseConf)
  val admin =  connection.getAdmin
  val tableName = conf.getString("hbase.tableName")
  val columnFamily = conf.getString("hbase.columnFamily")
  val putCount = conf.getInt("hbase.putCount")

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
      val counter = i.toString
      val rowKey = Bytes.toBytes(counter)
      val put = new Put(rowKey)
      val qualifier = Bytes.toBytes(counter)
      val value = Bytes.toBytes( (i + i).toString )
      put.addColumn(family, qualifier, value)
      puts += put
    }
    val table = connection.getTable(TableName.valueOf(tableName))
    table.put(puts.asJava)
    log.info(s"*** Put $putCount rows to table: $tableName")
  }

  def scan(): Try[IndexedSeq[String]] = Try {
    val filterList = new FilterList()
    filterList.addFilter(new FirstKeyOnlyFilter())
    filterList.addFilter(new KeyOnlyFilter())
    val scan = new Scan()
    scan.setFilter(filterList)
    val table = connection.getTable(TableName.valueOf(tableName))
    val scanner = table.getScanner(scan)
    val rowKeys = ArrayBuffer.empty[String]
    val iterator = scanner.iterator
    while ( iterator.hasNext ) {
      rowKeys += iterator.next.getRow.toString
    }
    rowKeys
  }

  def get(rowKey: String): Try[String] = Try {
    val get = new Get(Bytes.toBytes(rowKey))
    val table = connection.getTable(TableName.valueOf(tableName))
    val result = table.get(get)
    result.getRow.toString
  }
}