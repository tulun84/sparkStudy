package structuredStreaming.sink

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * @Author: junping.chi@luckincoffee.com
  * @Date: 2019/7/27 14:46
  * @Description:
  */
object ExpressionEncoderUtil {

  /**
    * 根据DataFrame创建encoder
    *
    * @param data dataFrame
    * @return ExpressionEncoder
    */
  def getEncoder(data: DataFrame): ExpressionEncoder[Row] = {
    RowEncoder(data.schema).resolveAndBind(data.queryExecution.analyzed.output, data.sparkSession.sessionState.analyzer)
  }

}
