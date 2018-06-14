package com.tulun.les19

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkSession 一句SQL解决UV+PV+secondRate
  * 核心：基于sessionid，不是基于userid计算
  */
object VisitCountBySparkSession {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("VisitCountBySparkSession").setMaster("local")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext
    /*导入隐式转换*/
    import ss.implicits._
    /*获取源数据*/
    val fileRDD = sc.textFile("file:///F:\\大数据视频\\Spark全套视频教程\\第03阶段—CDH5.7.1+Spark2.X（北风网高价购买+土伦整理）\\笔记、课件、资料\\第14讲、添加Hive服务及设置Mysql元数据库-资料\\data\\2015082818")
      .cache() //在读取数据之后马上就要cache,对之后的RDD进行cache不会起作用
      .filter(line => line.length > 0) //将空行过滤掉
      .map(line => {
      val arr = line.split("\t") //hive默认分隔符“\001”
      val date = arr(17).substring(0, 10) //trackTime的时间格式 2001-01-01 22:22:22
      val guid = arr(5) //用户id
      val sessionid = arr(10) //用户id
      val url = arr(1) //url
      (date, guid, sessionid, url) //返回三个元素的元组：Tuple3对象
    }).filter(line => line._4.length > 0) //初步抽取字段后，将url不存在的行过滤掉
      .toDF("date", "guid", "sessionid", "url")
      .persist(StorageLevel.MEMORY_ONLY) //初步整理完后，进行持久化

//    fileRDD.take(3).foreach(print)
    /*创建临时视图*/
    fileRDD.createOrReplaceTempView("log")
    /*构建sql语句*/

    val sql=s"""select date,sum(pv) pv,
         |count(distinct(guid)) uv,
         |count(case when pv>=2 then sessionid else null end) second_num,
         |count(sessionid) session_num
         |from (
         |select date,sessionid,max(guid) guid,count(url) pv from log
         |group by date,sessionid
         |) as s
         | group by date
       """.stripMargin
    val result = ss.sql(sql).cache()
    result.show() //预览查询结果
    result.printSchema() //打印表结构
    /*
      +----------+-----+-----+----------+-----------+
      |      date|   pv|   uv|second_num|session_num|
      +----------+-----+-----+----------+-----------+
      |2015-08-28|35880|16065|     6678|      16185|
      +----------+-----+-----+----------+-----------+

      root
       |-- date: string (nullable = true)
       |-- pv: long (nullable = true)
       |-- uv: long (nullable = false)
       |-- second_num: long (nullable = false)
       |-- session_num: long (nullable = false)
     */
//    val rows = result.rdd.collect()
//      .map(row=>{
//      val date = row.getAs[String]("date")
//      val uv = row.getAs[Long]("uv")
//      val pv = row.getAs[Long]("pv")
//      val second_num = row.getAs[Long]("second_num")
//      val session_num = row.getAs[Long]("session_num")
//      (date,uv,pv,second_num,session_num)
//    }).foreach(println)

    fileRDD.unpersist()
    sc.stop()
    ss.stop()
  }

}
