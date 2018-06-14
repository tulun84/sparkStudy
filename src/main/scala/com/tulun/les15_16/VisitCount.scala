package com.tulun.les15_16

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * UV、PV的计算
  * 1、单独计算UV
  * 2、单独计算PV
  * 3、UV、PV在一个方法里计算
  */
object VisitCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)



    /*说明：
      1、将window本地hosts加上CDH集群ip映射；
      2、将CDH集群的hdfs-site.xml放大项目的resources目录下,
         这样nameservice1才能识别，否则只能写active的namenode:端口模式(CDH默认设置namenode端口8022)
     */


    val fileRDD = sc.textFile("hdfs://nameservice1/user/hive/warehouse/track_log/ds=2015-08-28/hour=18")
    //val fileRDD = sc.textFile("file:///F:\\大数据视频\\Spark全套视频教程\\第03阶段—CDH5.7.1+Spark2.X（北风网高价购买+土伦整理）\\笔记、课件、资料\\第14讲、添加Hive服务及设置Mysql元数据库-资料\\data\\2015082818")
      .cache() //在读取数据之后马上就要cache,对之后的RDD进行cache不会起作用
      .filter(line => line.length > 0) //将空行过滤掉
      .map(line => {
      val arr = line.split("\t") //hive默认分隔符“\001”
      val date = arr(17).substring(0, 10) //trackTime的时间格式 2001-01-01 22:22:22
      val guid = arr(5) //用户id
      val url = arr(1) //url
      (date, guid, url) //返回三个元素的元组：Tuple3对象
    }).filter(line => line._3.length > 0) //初步抽取字段后，将url不存在的行过滤掉
      .persist(StorageLevel.DISK_ONLY) //初步整理完后，进行持久化

    //#fileRDD.take(3).foreach(println)

    /**
      * 1、计算uv
      * 核心：去重-groupbykey(3) //设置并行度(分区数)为3，测试数据量小，默认1个并行度就行
      * hive验证SQL:
      * select ds,count(distinct guid) from track_log
      * where ds=='2015-08-28' and hour=='18' and length(url)>0  group by ds;
      */
    val uvRDD = fileRDD.map(line => (line._1 + "_" + line._2, 1)).groupByKey(2) //格式 (date_guid,(1,1,,,,))
      .map(t => {
      val arr = t._1.split("_")
      val date = arr(0) //date
      (date, 1)
    }).reduceByKey(_ + _) //.foreach(println)

    /**
      * 2、计算PV:
      * 核心：fileRDD已将无效url过滤掉，现在只需要计算fileRDD的行数即为PV
      * select ds,count(distinct guid) uv count(url) pv from track_log
      * where ds=='2015-08-28' and hour=='18' and length(url)>0group by ds;
      */
    val pvRDD = fileRDD.map(line => (line._1, 1)).reduceByKey(_ + _) //.foreach(println)
    uvRDD.join(pvRDD).foreach(println) //(2015-08-28,(16068,35880))
    // .saveAsTextFile("hdfs://nameservice1/user/root/visitCount/") //写入DB、hdfs、sql写入hive表

    /**
      * 3、UV、PV 在一段代码实现两个指标的统计
      */

    fileRDD.map(line => (line._1 + "_" + line._2, 1)) //(data_guid,1)
      .reduceByKey(_ + _) //(date_guid,pvOfTheUser)
      .map(line => {
      val arr = line._1.split("_")
      val date = arr(0)
      val guid2One = if (arr(1).length > 0) 1 else 0 //key中的guid存在则记为一个UV，否则0个UV
      val pvOfTheUser = line._2 //当前用户的PV
      (date, (guid2One, pvOfTheUser))
    }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) //UV=x._1+y._1;PV=x._2+y._2
      .foreach(println) //(2015-08-28,(16068,35880))
    sc.stop()
  }

}
