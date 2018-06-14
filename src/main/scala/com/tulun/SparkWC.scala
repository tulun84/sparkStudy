package com.tulun

import org.apache.spark.{SparkConf, SparkContext}

object SparkWC {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val inputPath = "oss://news-zhibo8/bigdata_test/The_Sorrows_of_Young_Werther.txt"
    val outputPath = "oss://news-zhibo8/bigdata_test/output2"
    val numPartitions = 2

    val input = sc.textFile(inputPath, numPartitions)
    val output = input.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)

    output.saveAsTextFile(outputPath)
    sc.stop()
  }

}
