package spark

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object HBaseTableLoader {
  def put(connection: Connection, tableName: String, columnFamily: String, count: Int): Unit = {
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