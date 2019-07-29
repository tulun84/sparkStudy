package structuredStreaming.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow

/**
  * @Author: junping.chi@luckincoffee.com
  * @Date: 2019/7/27 15:00
  * @Description:
  */
object ForeachPartitionWriterUtil {
  def foreachPartitionWrite(data:DataFrame,function1: Function1[InternalRow,Unit])={

  }

}
