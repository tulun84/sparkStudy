package com.tulun.les15_16

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算二跳率（PV>2的session数量占比）
  * 1、二跳率计算公式：
  * rate = count(distinct case when pv>=2 then sessionid else null end) / count(distinct sessionid)
  * 2、url 为第2列，sessionid 为第11列，时间字段trackTime 为第18列
  */
object SecondRateCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)

    /*说明：
      1、将window本地hosts加上CDH集群ip映射；
      2、将CDH集群的hdfs-site.xml放到项目的resources目录下,
         这样nameservice1才能识别，否则只能写active的namenode:端口模式(CDH默认设置namenode端口8022)
    */

    /*
       获取源数据
     */

    // val fileRDD = sc.textFile("hdfs://nameservice1/user/hive/warehouse/track_log/ds=2015-08-28/hour=18")
    val fileRDD = sc.textFile("file:///F:\\大数据视频\\Spark全套视频教程\\第03阶段—CDH5.7.1+Spark2.X（北风网高价购买+土伦整理）\\笔记、课件、资料\\第14讲、添加Hive服务及设置Mysql元数据库-资料\\data\\2015082818")
      .cache() //在读取数据之后马上就要cache,对之后的RDD进行cache不会起作用
      .filter(line => line.length > 0) //将空行过滤掉
      .map(line => {
      val arr = line.split("\t") //hive默认分隔符“\001”
      val date = arr(17).substring(0, 10) //trackTime的时间格式 2001-01-01 22:22:22
      val sessionid = arr(10) //用户id
      val url = arr(1) //url
      (date, sessionid, url) //返回三个元素的元组：Tuple3对象
    }).filter(line => line._3.length > 0) //初步抽取字段后，将url不存在的行过滤掉
      .persist(StorageLevel.DISK_ONLY) //初步整理完后，进行持久化

    /*
       计算二跳率
     */
    val secondClickRDD=fileRDD.map(line => (line._1 + "_" + line._2, 1)).reduceByKey(_ + _) //(date_sessionid,pvOfSessionid)
      .map(t => {
      val arr = t._1.split("_")
      val date = arr(0)
      val sessionid2One = if (arr(1).length > 0) 1 else 0 //用于类加后得出总会话数
      val secondClick2One = if (t._2 > 0) 1 else 0 //用户累加后得出二跳总会话数
      (date, (sessionid2One, secondClick2One))
    }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) //(date,sessionidConut,secondClickSessionidCount)
    secondClickRDD.foreach(println)
    sc.stop()
  }

}
