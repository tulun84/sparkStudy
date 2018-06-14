package com.tulun.les27

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 分区计算：Partition
  * 1.会将相同的key存到同一个分区里
  * 2.小表（如维表：只会发生小量shuffle）不需要进行hashCode分区
  */
object PartitionVisitCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)

    //    val fileRDD = sc.textFile("hdfs://nameservice1/user/hive/warehouse/track_log/ds=2015-08-28/hour=18")
    val fileRDD = sc.textFile("file:///F:\\大数据视频\\Spark全套视频教程\\第03阶段—CDH5.7.1+Spark2.X（北风网高价购买+土伦整理）\\笔记、课件、资料\\第14讲、添加Hive服务及设置Mysql元数据库-资料\\data\\2015082818")
      .cache() //在读取数据之后马上就要cache,对之后的RDD进行cache不会起作用
      .filter(line => line.length > 0) //将空行过滤掉
      .map(line => {
      val arr = line.split("\t") //hive默认分隔符“\001”
      val date = arr(17).substring(0, 10) //trackTime的时间格式 2001-01-01 22:22:22
      val guid = arr(5) //用户id
      val url = arr(1) //url
      val id = arr(0) //url
      (id, (guid, url)) //返回三个元素的元组：Tuple3对象
    }).partitionBy(new HashPartitioner(10)) //采用hashcode分片方式，将数据分成10份（10个分区存储）
      .persist(StorageLevel.DISK_ONLY) //初步整理完后，进行持久化
    //进行join的两个RDD的key的类型必须一致
    val ids = List("121508281810000002","121508281810000003","121508281810000004")
    val RDD2 = sc.parallelize(ids,10).map(i => (i + "", i + "_name")) //RDD2如果是个小表，则不需要进行hashcode分区
    fileRDD.join(RDD2).foreach(println)
  }
}