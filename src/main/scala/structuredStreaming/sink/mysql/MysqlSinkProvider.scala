package structuredStreaming.sink.mysql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

/**
  *  * @Description: 注册mysql sink
  *  * @Author: yang.yang   yang.yang03@luckincoffee.com
  *  * @Date: 2019/4/17 11:08
  *  */
class MysqlSinkProvider  extends StreamSinkProvider with DataSourceRegister with Logging{

  val format = "mysql"

  override def shortName(): String = format

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    checkSinkParameters(parameters,outputMode)
    new MysqlSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode)
  }



  /**
    * 检查参数合法性
    * @param parameters 通过spark.writeStream.outputMode("append").format("mysql").option("tableName", "tableA")指定的参数
    */
  def checkSinkParameters(parameters: Map[String, String],outputMode:OutputMode) = {
    logInfo("outputMode :" + outputMode.toString)
    if(!outputMode.equals(OutputMode.Append())){
      throw new IllegalArgumentException(" mysql sink only support outputMode  is append");
    }
    if(parameters.get("tableName") == null) {
      throw new IllegalArgumentException("tableName is required, you can set topic like spark.writeStream.format(\"mysql\").option(\"tableName\", \"tableA\")")
    }
  }
}
