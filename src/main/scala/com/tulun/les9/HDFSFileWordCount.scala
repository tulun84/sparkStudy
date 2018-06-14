package com.tulun.les9

import org.apache.spark.{SparkConf, SparkContext}

object HDFSFileWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HDFSWordCount").setMaster("local")
    val sc = new SparkContext(conf)

    /*
       1.将CDH集群的hdfs-site.xml放到项目的resources目录下,这样nameservice1才能识别，
     */
//    val linesRDD = sc.textFile("hdfs://192.168.3.184:8022/user/admin/testAAAA/aaa.txt")
    val linesRDD = sc.textFile("hdfs://nameservice1/user/admin/testAAAA/aaa.txt")
    val word2Count3 = linesRDD.flatMap(line => line.split(",")).map((_, 1)).reduceByKey(_ + _)
    word2Count3.foreach(println)

    sc.stop()
  }
}
