package com.tulun.les9

import org.apache.spark.{SparkConf, SparkContext}

object LocalFileWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HDFSWordCount").setMaster("local")
    val sc = new SparkContext(conf)
    //
    val linesRDD = sc.textFile("file:///D:\\spark_testFile\\localFileWordCount.txt")
    val word2Count3 = linesRDD.flatMap(line => line.split(",")).map((_, 1)).reduceByKey(_ + _)
    word2Count3.foreach(println)

    sc.stop()
  }
}
