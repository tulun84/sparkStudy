package com.tulun.les32_33

import org.apache.spark.SparkConf
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 消费者consumer的WourdCount
  *
  */
object ConsumerWordCount {

  def main(args: Array[String]) {
    //    if (args.length < 4) {
    //      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
    //      System.exit(1)
    //    }
    //
    StreamingExamples.setStreamingLogLevels()
    //    val Array(zkQuorum, group, topics, numThreads) = args
    val zkQuorum = "CDH01:2181,CDH02:2181,CDH01:2183"
    val group = "g1" //对kafka来讲，groupid的作用是什么？
    /*
      对 kafka 来讲，groupid 的作用是什么？
      多个作业同时消费同一个 topic 时：
      1、每个作业拿到完整数据，计算互不干扰；
      2、每个作业拿到一部分数据，相当于进行了负载均衡；
      当多个作业 groupid 相同时，属于情况 2；
      否则属于情况 1.
     */
    val topics = "logTopic"
    val numThreads = 2

    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]") //核数至少给2，如果只是1的话，无法进行数据计算。
    val ssc = new StreamingContext(sparkConf, Seconds(2)) //2s一个批次(企业中多为20s/30s/1min等，否则压力大)

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //    val topicMap2 = Map(topics->2)
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) //每一个message或称作时间段批次

    val words = lines.flatMap(_.split(" ")) //1 6 0 7 9 1 9 5 8 5
    val wordCounts: DStream[(String, Int)] = words.map(x => (x, 1)).reduceByKey(_ + _) //得到每个批次内的(word,count)
    //wordCounts.print(3)  //每个批次打印3行(pint()默认打印10行)

    /*
      wordCounts里面存放的是一串RDD序列，是DStream对象
     */
    wordCounts.foreachRDD(rdd => {
      rdd.foreachPartition(p => {
        p.foreach(println)
      }
      )
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
