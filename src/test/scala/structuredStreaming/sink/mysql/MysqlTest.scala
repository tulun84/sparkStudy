package structuredStreaming.sink.mysql

import org.apache.spark.sql.execution.datasources.DataSource
import org.scalatest.FunSuite

/**
  * @Author: junping.chi@luckincoffee.com
  * @Date: 2019/7/27 11:08
  * @Description:
  */
class MysqlTest extends FunSuite {
  test("FlexqSource.load") {
    val clazz = DataSource.lookupDataSource("mysql", null)
    println(clazz.getCanonicalName)
  }

}
