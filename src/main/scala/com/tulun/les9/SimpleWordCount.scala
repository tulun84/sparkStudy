package com.tulun.les9

import org.apache.spark.{SparkConf, SparkContext}

object SimpleWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimpleWordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val words = Array("abc sd fd sd abc", "cdd cdd sd ee", "ee cs cs cs abc")
    val wordsRDD = sc.parallelize(words)
    //    val rdd2 = wordsRDD.flatMap(line=>line.split(" "))
    //    val rdd3 = rdd2.map(word=>(word,1))
    //    val word2Count = rdd3.reduceByKey(_+_)
    //    word2Count.foreach(println)

    val word2Count2 = wordsRDD.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)
    word2Count2.foreach(println)

    sc.stop()
  }

}
