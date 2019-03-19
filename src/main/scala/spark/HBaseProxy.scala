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

  def getRowKeys: Either[Throwable, Seq[String]] = Try {
    val admin = connection.getAdmin
    dropTable(admin)
    createTable(admin)
    val table = connection.getTable(TableName.valueOf(tableName))
    put(table)
    val rowKeys = scanRowKeys(table)
    table.close()
    admin.close()
    rowKeys
  }.toEither

  def getValues: Either[Throwable, Seq[String]] = Try {
    val admin = connection.getAdmin
    dropTable(admin)
    createTable(admin)
    val table = connection.getTable(TableName.valueOf(tableName))
    put(table)
    val values = scanValues(table)
    table.close()
    admin.close()
    values
  }.toEither

  def getValueByRowKey(rowKey: String): Either[Throwable, String] = Try {
    val table = connection.getTable(TableName.valueOf(tableName))
    val value = get(table, rowKey)
    table.close()
    value
  }.toEither

  def close(): Unit = connection.close()

  private def dropTable(admin: Admin): Unit = {
    val ifTableExists = TableName.valueOf(tableName)
    if (admin.tableExists(ifTableExists)) {
      admin.disableTable(ifTableExists)
      admin.deleteTable(ifTableExists)
    }
  }

  private def createTable(admin: Admin): Unit = {
    val table = TableName.valueOf(tableName)
    val column = ColumnFamilyDescriptorBuilder.of(columnFamily)
    val descripter = TableDescriptorBuilder.newBuilder(table).setColumnFamily(column).build()
    admin.createTable(descripter)
    log.info(s"*** Created table: $tableName")
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
    log.info(s"*** Put $putCount rows to table: $tableName")
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
      rowKeys += Bytes.toString(result.value)
    }
    scanner.close()
    log.info(s"*** Scan ${rowKeys.length} rows from table: $tableName")
    log.info(s"*** Row Keys: ${rowKeys.toString}")
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
    log.info(s"*** Scan ${values.length} rows from table: $tableName")
    log.info(s"*** Values: ${values.toString}")
    values
  }

  private def get(table: Table, rowKey: String): String = {
    val get = new Get(Bytes.toBytes(rowKey))
    val value = table.get(get).getRow.toString
    log.info(s"*** Get $rowKey from table: $tableName with value: $value")
    value
  }
}