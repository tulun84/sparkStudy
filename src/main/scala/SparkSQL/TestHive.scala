package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * SparkSession 一句SQL解决UV+PV+secondRate
  * 核心：基于sessionid，不是基于userid计算
  */
object TestHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestHive")//.setMaster("local")
    val ss = SparkSession.builder()
      .config(conf)
      .enableHiveSupport() //声明sparksession操作的是Hive
      .getOrCreate()

    val sc = ss.sparkContext
    /*导入隐式转换*/
    val date = "2015-08-28" //实际是通过传参进来
    val hour = "18" //实际是通过传参进来

    val sql =
      s"""select * from country_statistics_hive""".stripMargin

    println("Executing...SQL: "+sql)
    val result = ss.sql(sql).cache()
    result.show() //预览查询结果
    result.printSchema() //打印表结构

    sc.stop()
    ss.stop()
  }

}
