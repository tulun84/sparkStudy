package structuredStreaming.sink.mysql

import java.sql.Connection

import com.mysql.jdbc.MySQLConnection
import org.apache.commons.dbcp.DelegatingConnection
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{ForeachWriter, Row}
import structuredStreaming.sink.util.DataSourcePool

/**
  *  * @Description: write into Mysql
  *  * @Author: yang.yang03@luckincoffee.com
  *  * @Date: 2019/4/17 14:16
  *  */
class MysqlWriter(mysqlConfig :String ,tableName: String,schema: StructType)  extends ForeachWriter[Iterator[Row]] with Logging with Serializable {

  //一个事务提交条数
  val commitCount = 1000

  override def open(partitionId: Long, epochId: Long): Boolean = {
    true
  }

  override def process(tableData:Iterator[Row]): Unit = {
    logInfo("tableName:" + tableName)
    if (tableData.isEmpty) {
      logInfo("return by empty :")
      return
    }
    insertRows(tableData, tableName)
  }

  /**
    * 获取mysql连接信息
    * @return mysql连接
    */
  def getConnection(): Connection = {
    val conn: Connection = DataSourcePool.getDataSource(mysqlConfig).getConnection
    conn.setAutoCommit(false)
    if(conn.isInstanceOf[DelegatingConnection]) {
      val delegatingConnection = conn.asInstanceOf[DelegatingConnection]
      if(delegatingConnection.getDelegate.isInstanceOf[MySQLConnection]) {
        val mysqlConn = delegatingConnection.getDelegate.asInstanceOf[MySQLConnection]
        mysqlConn.setRewriteBatchedStatements(true)
        mysqlConn.setCachePrepStmts(true)
      }
    }
    conn
  }

  def insertRows(tableData: Iterator[Row], tableName: String): Unit = {

    val fieldNameArr =  schema.fieldNames.toList

    val conn = getConnection()

    //每个批次索引
    var numData = 0
    //循环到 tableData的索引位置
    var indexData = 0
    //column str
    val columnStr = new StringBuilder
    val placeholder =  new StringBuilder
    for (i <- 0 until fieldNameArr.size) {
      columnStr.append(fieldNameArr(i)).append(",")
      placeholder.append("?,")
    }

    // 创建sql前缀
    val sql :String = "INSERT INTO "+tableName+"("+columnStr.substring(0, columnStr.length() - 1)+")"+" VALUES (" +placeholder.substring(0, placeholder.length() - 1) +")"

    val pstm = conn.prepareStatement(sql)

    while (tableData.hasNext) {
      numData = numData + 1
      indexData = indexData + 1
      val row: Row = tableData.next()
      logInfo("row  value :" + row)
      for (i <- 1 to fieldNameArr.size) {
        val fieldValue = row.get(i - 1)
        if (fieldValue == null) {
          pstm.setObject(i, fieldValue)
        } else {
          schema.fields(i - 1).dataType match {
            case DataTypes.StringType => pstm.setString(i, row.getString(i - 1))
            case DataTypes.BooleanType => pstm.setBoolean(i, row.getBoolean(i - 1))
            case DataTypes.IntegerType => pstm.setInt(i, row.getInt(i - 1))
            case DataTypes.TimestampType => pstm.setTimestamp(i, row.getTimestamp(i - 1))
            case DataTypes.DateType => pstm.setDate(i, row.getDate(i - 1))
            case DataTypes.ByteType => pstm.setByte(i, row.getByte(i - 1))
            case DataTypes.LongType => pstm.setLong(i, row.getLong(i - 1))
            case DataTypes.FloatType => pstm.setFloat(i, row.getFloat(i - 1))
            case DataTypes.DoubleType => pstm.setDouble(i, row.getDouble(i - 1))
            case DataTypes.ShortType => pstm.setShort(i, row.getShort(i - 1))
            case t => throw new IllegalArgumentException(s"No support for mysql writer type $t")
          }
        }

      }
      pstm.addBatch()
      if (numData == commitCount) {
        pstm.executeBatch
        conn.commit
        //重置批次索引,和拼接的sql
        numData = 0
      }
    }
    if(numData > 0) {
      pstm.executeBatch
      conn.commit
    }
    conn.close()
  }


  override def close(errorOrNull: Throwable): Unit = {
    if (errorOrNull != null) {
      logError("error happens when write to mysql", errorOrNull)
    }
  }

}

object MysqlWriter{
  def apply(mysqlConfig :String ,tableName: String,schema: StructType)  = new MysqlWriter(mysqlConfig :String ,tableName: String,schema: StructType)

}

