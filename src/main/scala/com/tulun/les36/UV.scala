package com.tulun.les36

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 流式计算：
  * 计算今天截止当前的UV数
  */
object UV {

  /**
    * count(dsintct guid) group by date
    * 借助set完成去重，检查点里存全部guid
    *
    */
  def main(args: Array[String]) {
    //      if (args.length < 2) {
    //        System.exit(1)
    //      }
    StreamingExamples.setStreamingLogLevels()
    val checkpointDirectory = "hdfs://nameservice1/user/root/checkpoint/uv2"

    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {

        //val Array(brokers, topics) = args
        val topics = "logTopic"
        val brokers = "CDH01:9092,CDH02:9092,CDH03:9092"
        val zkQuorum = "CDH01:2181,CDH02:2181,CDH03:2181"
        val group = "g1"

        val sparkConf = new SparkConf().setAppName("uv").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(2))
        ssc.checkpoint(checkpointDirectory)

        // Create direct kafka stream with brokers and topics
        val topicsSet = topics.split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

        //不经过zk，直接消费kafka
        val messages2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, topicsSet)

        val topicMap = topics.split(",").map((_, 2)).toMap
        //val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

        //computePcUv(messages)
        computeUv(messages2)
        ssc
      })

    ssc.start()
    ssc.awaitTermination()
  }

  def updateUvState(values: Seq[Set[String]], state: Option[Set[String]]): Option[Set[String]] = {
    //Set(guid)
    val defaultState = Set[String]()
    values match {
      case Nil => Some(state.getOrElse(defaultState)) //如果为空（首个批次）. Nil是一个空的List,定义为List[Nothing]
      case _ =>
        val guidSet = values.reduce(_ ++ _) //set与set拼接用++ ，set里的元素自动去重。
        println("11111-" + state.getOrElse(defaultState).size)
        println("22222-" + guidSet.size)
        Some(state.getOrElse(defaultState) ++ guidSet)
    }
  }

  def computeUv(messages: InputDStream[(String, String)]) = {
    val sourceLog = messages.map(_._2) //messages里是Touple2，第二列是数据。  2016-09-10 12:42:38,id_44,www.yhd.com/aa7

    val utmUvLog = sourceLog.filter(_.split(",").size == 3)
      .map(logInfo => {
        val arr = logInfo.split(",")
        val date = arr(0).substring(0, 10)
        val guid = arr(1)
        //      val url = arr(2)
        (date, Set(guid))
      }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val utmDayActive = utmUvLog.updateStateByKey(updateUvState). // date,set(guid)
      map(result => {
      (result._1, result._2.size) // date,UV
    }).print()


    //遍历和查看结果

  }
}
