package com.tulun.les13

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ActionTest").setMaster("local")
    val sc = new SparkContext(conf)

    val table1 = sc.parallelize(List(("k1",10),("k2",20),("k2",15),("k3",15)))    // pairRDD里的Tuple2
    val table2 = sc.parallelize(List(("k1",11),("k2",12),("k1",100)))
//join
    table1.join(table2).foreach(println)
    table1.leftOuterJoin(table2).foreach(println)

    table1.groupByKey().foreach(println)
    table1.reduceByKey(_+_).foreach(println)

    sc.stop()
  }

}
