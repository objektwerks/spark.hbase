package spark

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, HBaseAdmin, TableDescriptorBuilder}

object HBaseTableBuilder {
  def build(admin: HBaseAdmin, tableName: String, columnFamily: String): Unit = {
    val table = TableName.valueOf(tableName)
    val column = ColumnFamilyDescriptorBuilder.of(columnFamily)
    val descripter = TableDescriptorBuilder.newBuilder(table).setColumnFamily(column).build()
    admin.createTable(descripter)
  }
}