package com.tulun.les26

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object OperJsonAndParquet {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UseUDF").setMaster("local[2]")
    val ss = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val sc = ss.sparkContext

    import ss.implicits._
    //val fileRDD = sc.textFile("file:///F:\\大数据视频\\Spark全套视频教程\\第03阶段—CDH5.7.1+Spark2.X（北风网高价购买+土伦整理）\\笔记、课件、资料\\第14讲、添加Hive服务及设置Mysql元数据库-资料\\data\\2015082818")

    ss.read.json("src/main/testFiles/people.json").createOrReplaceTempView("people")
    ss.sql("select * from people").rdd.foreach(println)

    ss.read.parquet("src/main/testFiles/users.parquet").createOrReplaceTempView("users")
    ss.sql("select * from users").rdd.foreach(println)

    ss.read.text("src/main/testFiles/people.txt").createOrReplaceTempView("people2")
    ss.sql("select * from people2").rdd.foreach(println)

    sc.stop()
    ss.stop()
  }

}
