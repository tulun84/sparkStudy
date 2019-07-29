package structuredStreaming.sink.mysql

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConversions._

/**
  *  * @Description: MysqlSink
  *  * @Author:  yang.yang03@luckincoffee.com
  *  * @Date: 2019/4/17 11:09
  *  */
class MysqlSink  (sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode) extends Sink with Logging with Serializable {

  private val EMPTY = ""

  private val tableName = parameters.get("tableName").map(_.toString).getOrElse(EMPTY)

  private val registerKey = parameters.get("registerKey").map(_.toString).getOrElse(EMPTY)

  private val mysqlConfig = ConfigCenterUtil.getByKey(registerKey)


  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val encoder = RowEncoder(data.schema).resolveAndBind(
      data.queryExecution.analyzed.output,
      data.sparkSession.sessionState.analyzer)
    if (mysqlConfig== null) {
      throw new IllegalArgumentException("Please check ConfigCenter shipyard mysqlConfig key")
    }
    val writer = MysqlWriter(mysqlConfig, tableName,  data.schema)
    data.queryExecution.toRdd.foreachPartition(new ForeachWriteFunction(encoder, sqlContext, writer, batchId))

  }
}

class ForeachWriteFunction(encoder: ExpressionEncoder[Row], sqlContext: SQLContext, writer: MysqlWriter, batchId: Long) extends Function1[Iterator[InternalRow], Unit] with Logging with Serializable {
  override def apply(iter: Iterator[InternalRow]): Unit = {
    val rows: java.util.List[Row] = new util.ArrayList[Row]()
    var count = 1
    try {
      while (iter.hasNext) {
        val internalRow = iter.next()
        val row = encoder.fromRow(internalRow)
        rows.add(row)
        count += 1
      }
    } catch {
      case e: Throwable => {
        logError("ForeachWriteFunction error:" + e)
        throw e
      }
    }
    logInfo("start  process BatchCount :" + count)
    writer.process(rows.iterator)
  }
}