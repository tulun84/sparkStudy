package com.tulun.les20

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkSession 一句SQL解决UV+PV+secondRate
  * 核心：基于sessionid，不是基于userid计算
  */
object VisitCountBySparkSession4Hive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("VisitCountBySparkSession4Hive")//.setMaster("local")
    val ss = SparkSession.builder()
      .config(conf)
      .enableHiveSupport() //声明sparksession操作的是Hive
      .getOrCreate()

    val sc = ss.sparkContext
    /*导入隐式转换*/
    import ss.implicits._
    val date = "2015-08-28" //实际是通过传参进来
    val hour = "18" //实际是通过传参进来

    val sql =
      s"""INSERT OVERWRITE TABLE daily_visit partition (date="$date")
         |select sum(pv) pv,
         |count(distinct(guid)) uv,
         |count(case when pv>=2 then sessionid else null end) second_num,
         |count(sessionid) session_num
         |from (
         |select substr(ds,1,10) date,sessionid,max(guid) guid,count(url) pv from track_log
         |where ds="$date" and hour="$hour" and length(url)>0
         |group by substr(ds,1,10),sessionid
         |) as s
         | group by date
       """.stripMargin
    println("Executing...SQL: "+sql)
    val result = ss.sql(sql).cache()
    result.show() //预览查询结果
    result.printSchema() //打印表结构

    sc.stop()
    ss.stop()
  }

}
