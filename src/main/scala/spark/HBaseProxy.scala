package spark

import com.typesafe.config.Config
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{FilterList, FirstKeyOnlyFilter, KeyOnlyFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object HBaseProxy {
  def apply(conf: Config): HBaseProxy = new HBaseProxy(conf)
}

class HBaseProxy(conf: Config) {
  val log = Logger.getLogger(getClass.getName)
  val tableName = conf.getString("hbase.tableName")
  val columnFamily = conf.getString("hbase.columnFamily")
  val columnFamilyAsBytes = Bytes.toBytes(columnFamily)
  val valueQualifierAsBytes = Bytes.toBytes(conf.getString("hbase.valueQualifier"))
  val putCount = conf.getInt("hbase.putCount")
  val connection = ConnectionFactory.createConnection(HBaseConfiguration.create)

  def getRowKeys: Try[Seq[String]] = Try {
    val admin = connection.getAdmin
    dropTable(admin)
    createTable(admin)
    val table = connection.getTable(TableName.valueOf(tableName))
    put(table)
    val rowKeys = scanRowKeys(table)
    table.close()
    admin.close()
    rowKeys
  }

  def getValues: Try[Seq[String]] = Try {
    val admin = connection.getAdmin
    dropTable(admin)
    createTable(admin)
    val table = connection.getTable(TableName.valueOf(tableName))
    put(table)
    val values = scanValues(table)
    table.close()
    admin.close()
    values
  }

  def getValueByRowKey(rowKey: String): Try[String] = Try {
    val table = connection.getTable(TableName.valueOf(tableName))
    val value = get(table, rowKey)
    table.close()
    value
  }

  def close(): Unit = connection.close()

  private def dropTable(admin: Admin): Unit = {
    val ifTableExists = TableName.valueOf(tableName)
    if (admin.tableExists(ifTableExists)) {
      admin.disableTable(ifTableExists)
      admin.deleteTable(ifTableExists)
      log.info(s"*** HBaseProxy: Dropped table: $tableName")
    }
  }

  private def createTable(admin: Admin): Unit = {
    val table = TableName.valueOf(tableName)
    val column = ColumnFamilyDescriptorBuilder.of(columnFamily)
    val descripter = TableDescriptorBuilder.newBuilder(table).setColumnFamily(column).build()
    admin.createTable(descripter)
    log.info(s"*** HBaseProxy: Created table: $tableName")
  }

  private def put(table: Table): Unit = {
    val puts = ArrayBuffer.empty[Put]
    for (i <- 1 to putCount) {
      val counter = i.toString
      val rowKey = Bytes.toBytes(counter)
      val put = new Put(rowKey)
      val keyValue = KeyValue(counter, counter)
      val json = Json.toJson(keyValue).toString
      val value = Bytes.toBytes(json)
      put.addColumn(columnFamilyAsBytes, valueQualifierAsBytes, value)
      puts += put
    }
    table.put(puts.asJava)
    log.info(s"*** HBaseProxy: Put $putCount rows to table: $tableName")
  }

  private def scanRowKeys(table: Table): Seq[String] = {
    val filterList = new FilterList()
    filterList.addFilter(new FirstKeyOnlyFilter())
    filterList.addFilter(new KeyOnlyFilter())
    val scan = new Scan()
    scan.setFilter(filterList)
    val rowKeys = ArrayBuffer.empty[String]
    val scanner = table.getScanner(scan)
    for (result: Result <- scanner.iterator.asScala) {
      rowKeys += Bytes.toString(result.getRow)
    }
    scanner.close()
    log.info(s"*** HBaseProxy: Scan ${rowKeys.length} rows from table: $tableName")
    log.info(s"*** HBaseProxy: Row Keys: ${rowKeys.toString}")
    rowKeys
  }

  private def scanValues(table: Table): Seq[String] = {
    val scan = new Scan()
    scan.addColumn(columnFamilyAsBytes, valueQualifierAsBytes)
    val scanner = table.getScanner(scan)
    val values = ArrayBuffer.empty[String]
    for (result: Result <- scanner.iterator.asScala) {
      values += Bytes.toString(result.value)
    }
    scanner.close()
    log.info(s"*** HBaseProxy: Scan ${values.length} rows from table: $tableName")
    log.info(s"*** HBaseProxy: Values: ${values.toString}")
    values
  }

  private def get(table: Table, rowKey: String): String = {
    val get = new Get(Bytes.toBytes(rowKey))
    val value = Bytes.toString(table.get(get).value)
    log.info(s"*** HBaseProxy: Get $rowKey from table: $tableName with value: $value")
    value
  }
}