package zhibo8

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * useini表:
  * "id","udid","usercode","platform","name","val","createtime","os"
  * key:usercode,计算每个用户的“视频默认显示录像”、是否显示弹幕、获取比分推送的百分比
  */
object AnalyzUserIni3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AnalyzUserIni3").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext


    val fileRDD = ss.read.csv("C:\\Users\\cjp\\Desktop\\0330\\user_ini03_aggr.csv")
      .toDF("usercode","name","vall","count","usercode","sum","ratio")
      .createOrReplaceTempView("userconf")
    val sql =
      s"""
         |select count(1) from userconf
       """.stripMargin

    val result = ss.sql(sql).cache()

    result.show()

    ss.stop()
    sc.stop()
  }
}
