package com.tulun.les35

import org.apache.spark.SparkConf
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  *
  */
object AreaAmtByWindow {

  def main(args: Array[String]) {
    //    if (args.length < 4) {
    //      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
    //      System.exit(1)
    //    }
    //
    StreamingExamples.setStreamingLogLevels()
    //    val Array(zkQuorum, group, topics, numThreads) = args
    val zkQuorum = "CDH01:2181,CDH02:2181,CDH01:2183"
    val group = "g1"
    val topics = "orderTopic"
    val numThreads = 2

    val sparkConf = new SparkConf().setAppName("Direct").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("hdfs://nameservice1/user/root/checkpoint/Direct") //设置有状态的检查点

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val linerdd = lines.map { row => {
      val arr = row.split(",")
      val key = arr(3).substring(0, 10) + "_" + arr(0) //2016-09-04_Areaid
      val amt = arr(2).toInt
      (key, amt)
    }
    }.reduceByKeyAndWindow(
      _ + _, //加上新进入窗口的批次中的元素
      _ - _, //移除离开窗口的老批次中的元素
      Seconds(10), //窗口时长(去最近N时间段内的数据，应当设置为批次时间间隔的整数倍)
      Seconds(2), //滑动步长(DStream流的批次时间间隔)
      2)

    linerdd.print()


    ssc.start()
    ssc.awaitTermination()
  }

}
