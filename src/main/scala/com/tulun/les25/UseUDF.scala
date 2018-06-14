package com.tulun.les25

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1.spark的UDF不同于Hive的UDF——不能复用
  * 2.spasrk的UDF一般不需要用，一般其功能可在map中实现
  */
object UseUDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UseUDF").setMaster("local[2]")
    val ss = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val sc = ss.sparkContext

    import ss.implicits._
    val fileRDD = sc.textFile("file:///F:\\大数据视频\\Spark全套视频教程\\第03阶段—CDH5.7.1+Spark2.X（北风网高价购买+土伦整理）\\笔记、课件、资料\\第14讲、添加Hive服务及设置Mysql元数据库-资料\\data\\2015082818")
      .filter(_.length > 0)
      .map(line => {
        val arr = line.split("\t")
        val date = arr(17).substring(0, 10)
        val guid = arr(5)
        val url = arr(1)
        (date, guid, url)
      }).filter(_._3.length > 0)
      .persist(StorageLevel.DISK_ONLY)

    //开发一个同getTopic的UDF
    ss.udf.register("getName", (url: String, regex: String) => {
      if (url == null || url.trim.length == 0) null
      val p: Pattern = Pattern.compile(regex)
      val m: Matcher = p.matcher(url)
      if (m.find) {
        m.group(0).toLowerCase.split("\\/")(1)
      } else {
        null
      }
    })

    //注册临时视图
    fileRDD.toDF("date", "guid", "url").createOrReplaceTempView("log")
    val sql =
      s"""
         |select date,getName(url,'sale/([a-zA-Z0-9]+)'),count(distinct guid) uv,count(url) pv
         |from log
         |where url like '%sale%'
         |group by date,getName(url,'sale/([a-zA-Z0-9]+)')
       """.stripMargin
    ss.sql(sql).rdd.foreach(println)
    sc.stop()
    ss.stop()
  }

}
