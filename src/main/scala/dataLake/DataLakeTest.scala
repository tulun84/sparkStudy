package dataLake

import org.apache.spark.sql.SparkSession

/**
  * @Author: junping.chi@luckincoffee.com
  * @Date: 2019/7/24 18:11
  * @Description:
  */
object DataLakeTest {
  def main(args: Array[String]): Unit = {
    //先从官网的例子开始：
    val ss = SparkSession.builder().master("local[4]").getOrCreate()
    val data = ss.range(10, 15)
    //create a table
    //data.write.format("delta").save("/tmp/delta-table")
    //update the table data
    //data.write.format("delta").option("mergeSchema","true").mode("overwrite").save("/tmp/delta-table")
    //Read data
    ss.read.format("delta").load("/tmp/delta-table").show(false)
    //用Time Travel
    ss.read.format("delta").option("versionAsof", 1).load("/tmp/delta-table").show(false)
  }
}
