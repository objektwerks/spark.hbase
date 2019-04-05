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
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def apply(conf: Config): HBaseProxy = new HBaseProxy(conf)
}

class HBaseProxy(conf: Config) extends Serializable {
  import HBaseProxy.log

  val tableName = conf.getString("tableName")
  val columnFamily = conf.getString("columnFamily")
  val columnFamilyAsBytes = Bytes.toBytes(columnFamily)
  val valueQualifierAsBytes = Bytes.toBytes(conf.getString("valueQualifier"))
  val putCount = conf.getInt("putCount")

  def getRowKeys: Try[Seq[String]] = Try {
    val hbaseConf = HBaseConfiguration.create
    hbaseConf.set("zookeeper.quorum", conf.getString("zookeeperQuorum"))
    hbaseConf.set("zookeeper.property.clientPort", conf.getString("zookeeperClientPort"))
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    log.info("*** HBaseProxy: Connection created.")
    dropTable(admin)
    createTable(admin)
    val table = connection.getTable(TableName.valueOf(tableName))
    put(table)
    val rowKeys = scanRowKeys(table)
    table.close()
    admin.close()
    connection.close()
    log.info("*** HBaseProxy: Connection closed.")
    rowKeys
  }

  def getValueByRowKey(rowKey: String): Try[String] = Try {
    val hbaseConf = HBaseConfiguration.create
    hbaseConf.set("zookeeper.quorum", conf.getString("zookeeperQuorum"))
    hbaseConf.set("zookeeper.property.clientPort", conf.getString("zookeeperClientPort"))
    val connection = ConnectionFactory.createConnection(hbaseConf)
    log.info("*** HBaseProxy: Connection created.")
    val table = connection.getTable(TableName.valueOf(tableName))
    val value = get(table, rowKey)
    table.close()
    connection.close()
    log.info("*** HBaseProxy: Connection closed.")
    value
  }

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
      val counter = i
      val rowKey = Bytes.toBytes(counter)
      val put = new Put(rowKey)
      val keyValue = KeyValue(counter.toString, counter)
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

  private def get(table: Table, rowKey: String): String = {
    val get = new Get(Bytes.toBytes(rowKey))
    val value = Bytes.toString(table.get(get).value)
    log.info(s"*** HBaseProxy: Get $rowKey from table: $tableName with value: $value")
    value
  }
}