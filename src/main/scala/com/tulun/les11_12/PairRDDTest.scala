package com.tulun.les11_12

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object PairRDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ActionTest").setMaster("local")
    val sc = new SparkContext(conf)
    //    val numListRDD = sc.parallelize(1 to 5, 2) //将数据分在2个分区里
    val numListRDD = sc.parallelize(List(1, 2, 2, 1, 1, 3, 3, 3, 2, 2, 21, 5, 5, 5, 4)) //将数据分在2个分区里
    val pairRDD = numListRDD.map(x => {
      if (x % 2 == 0) {
         (x, x + 2) //返回形式不一定是tuple2类型的k-v对,也可以是"(x+2)"返回单元素
      } else {
         (x, x + 1)
      }
    }).persist(StorageLevel.MEMORY_ONLY)

    val mapRDD = pairRDD.map(t => (t._1 + 1, t._2 + 2))
    //    val mapRDD = pairRDD.map { case (x, y) => (x + 1, y + 2) }//偏函数写法，注意map{}是花括号，不是小括号
    val mapRDDFiltered = mapRDD.filter { case (x, y) => x > 4 }

    val mapRDDMapvalued = mapRDD.mapValues(value => value + 1) //所有value+1

    println(pairRDD.collect().toBuffer)

    //求 pairRDD 中相同key的value的均值：sum(values)/count(key)
    pairRDD.mapValues(v => (v, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(t => (t._1 / t._2))
      .sortByKey()
      .foreach(println)



    sc.stop()
  }

}
