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
    var table: Option[Table] = None
    try {
      table = Some( connection.getTable(TableName.valueOf(tableName)) )
      table.foreach( t => t.put(puts.asJava) )
    } finally {
      table.foreach( t => t.close() )
      log.info(s"*** Put $putCount rows to table: $tableName")
    }
  }

  def scan(): Try[IndexedSeq[String]] = Try {
    val filterList = new FilterList()
    filterList.addFilter(new FirstKeyOnlyFilter())
    filterList.addFilter(new KeyOnlyFilter())
    val scan = new Scan()
    scan.setFilter(filterList)
    var table: Option[Table] = None
    val rowKeys = ArrayBuffer.empty[String]
    try {
      table = Some( connection.getTable(TableName.valueOf(tableName)) )
      val scanner = table.map( t => t.getScanner(scan) )
      scanner.foreach { s =>
        val iterator = s.iterator
        while ( iterator.hasNext ) {
          rowKeys += iterator.next.getRow.toString
        }
      }
      rowKeys
    } finally {
      table.foreach( t => t.close() )
      log.info(s"*** Scan ${rowKeys.length} rows from table: $tableName")
    }
  }

  def get(rowKey: String): Try[Option[String]] = Try {
    val get = new Get(Bytes.toBytes(rowKey))
    var table: Option[Table] = None
    try {
      table = Some( connection.getTable(TableName.valueOf(tableName)) )
      table.map( t => t.get(get).getRow.toString )
    } finally {
      table.foreach( t => t.close() )
      log.info(s"*** Get $rowKey from table: $tableName")
    }
  }
}