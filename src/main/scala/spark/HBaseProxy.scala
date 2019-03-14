package spark

import com.typesafe.config.Config
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{FilterList, FirstKeyOnlyFilter, KeyOnlyFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

case class HBaseProxy(conf: Config) {
  val log = Logger.getLogger(getClass.getName)
  val tableName = conf.getString("hbase.tableName")
  val columnFamily = conf.getString("hbase.columnFamily")
  val putCount = conf.getInt("hbase.putCount")
  val connection = ConnectionFactory.createConnection(HBaseConfiguration.create)

  def getRowKeys: Either[Throwable, Seq[String]] = Try {
    val admin = connection.getAdmin
    createTable(admin)
    val table = connection.getTable(TableName.valueOf(tableName))
    put(table)
    val rowKeys = scan(table)
    table.close()
    admin.close()
    rowKeys
  }.toEither

  def getValueByRowKey(rowKey: String): Either[Throwable, String] = Try {
    val table = connection.getTable(TableName.valueOf(tableName))
    val value = get(table, rowKey)
    table.close()
    value
  }.toEither

  def close(): Unit = connection.close()

  private def createTable(admin: Admin): Unit = {
    val table = TableName.valueOf(tableName)
    val column = ColumnFamilyDescriptorBuilder.of(columnFamily)
    val descripter = TableDescriptorBuilder.newBuilder(table).setColumnFamily(column).build()
    admin.createTable(descripter)
    log.info(s"*** Created table: $tableName")
  }

  private def put(table: Table): Unit = {
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
    table.put(puts.asJava)
    log.info(s"*** Put $putCount rows to table: $tableName")
  }

  private def scan(table: Table): Seq[String] = {
    val filterList = new FilterList()
    filterList.addFilter(new FirstKeyOnlyFilter())
    filterList.addFilter(new KeyOnlyFilter())
    val scan = new Scan()
    scan.setFilter(filterList)
    val rowKeys = ArrayBuffer.empty[String]
    val scanner = table.getScanner(scan)
    val iterator = scanner.iterator
    while ( iterator.hasNext ) {
      rowKeys += iterator.next.getRow.toString
    }
    log.info(s"*** Scan ${rowKeys.length} rows from table: $tableName")
    rowKeys
  }

  private def get(table: Table, rowKey: String): String = {
    val get = new Get(Bytes.toBytes(rowKey))
    val value = table.get(get).getRow.toString
    log.info(s"*** Get $rowKey from table: $tableName with value: $value")
    value
  }
}