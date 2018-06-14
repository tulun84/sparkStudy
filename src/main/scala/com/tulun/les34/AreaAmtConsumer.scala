package com.tulun.les34

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 按天计算每个地区的销售额
  * （维度：天、地区；指标：销售额）
  */
object AreaAmtConsumer {

  def main(args: Array[String]) {
    //    if (args.length < 4) {
    //      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
    //      System.exit(1)
    //    }
    //
    //    StreamingExamples.setStreamingLogLevels()
    //    val Array(zkQuorum, group, topics, numThreads) = args
    val zkQuorum = "CDH01:2181,CDH02:2181,CDH01:2183"
    val group = "g1"
    val topics = "orderTopic"
    val numThreads = 2

    val sparkConf = new SparkConf().setAppName("StatelessWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("hdfs://nameservice1/user/root/checkpoint/AreaAmt") //设置有状态的检查点

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    //lines.print(3)

    //产生我们需要的pairRDD(其中的key为维度组合)
    val linerdd = lines.map { row => {
      // row：地区id，订单id，订单金额，订单时间
      val arr = row.split(",")
      val key = arr(3).substring(0, 10) + "_" + arr(0) //2016-09-04_Areaid  ,扩展：继续细分到城市
      val amt = arr(2).toInt
      (key, amt)
    }
    }

    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq,再计算每个key的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0)
      // 返回累加后的结果，是一个Option[Int]类型
      Some(currentCount + previousCount)
    }

    linerdd.updateStateByKey(addFunc).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
