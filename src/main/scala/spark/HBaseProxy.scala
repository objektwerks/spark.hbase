package spark

import com.typesafe.config.Config
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

class HBaseProxy(conf: Config) {
  val hbaseConf = HBaseConfiguration.create
  val connection = ConnectionFactory.createConnection(hbaseConf)
  val admin =  connection.getAdmin
  val tableName = conf.getString("hbase.table-name")
  val columnFamily = conf.getString("hbase.column-family")

  def build(): Unit = {
    val table = TableName.valueOf(tableName)
    val column = ColumnFamilyDescriptorBuilder.of(columnFamily)
    val descripter = TableDescriptorBuilder.newBuilder(table).setColumnFamily(column).build()
    admin.createTable(descripter)
  }

  def put(count: Int): Unit = {
    val puts = ArrayBuffer.empty[Put]
    for (i <- 1 to count) {
      val columnFamilyAsBytes = columnFamily.getBytes
      val valueAsString = i.toString
      val valueAsBytes = valueAsString.getBytes
      val row = Bytes.toBytes(valueAsString)
      val put = new Put(row)
      put.addColumn(columnFamilyAsBytes, valueAsBytes, valueAsBytes)
      puts += put
    }
    val table = connection.getTable(TableName.valueOf(tableName))
    table.put(puts.asJava)
  }
}

object HBaseProxy {
  def apply(conf: Config): HBaseProxy = new HBaseProxy(conf)
}