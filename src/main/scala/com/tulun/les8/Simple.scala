package com.tulun.les8

import org.apache.spark.{SparkConf, SparkContext}

object Simple {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("simple").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5))
    //map对每个元素处理，返回相同类型
    val map2Filter = rdd.map(_ + 1).filter(_ > 3) //map、filter是transform
    val map2Filter2 = rdd.map(_ + "_Test")
    println(map2Filter.count()+"==="+map2Filter.first()) //第一个 Action
    println(map2Filter2.count())
    //    map2Filter.foreach(println)
    //    map2Filter2.foreach(println)
    sc.stop()
  }
}
