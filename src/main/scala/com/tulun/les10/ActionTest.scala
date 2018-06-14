package com.tulun.les10

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object ActionTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ActionTest").setMaster("local")
    val sc = new SparkContext(conf)
    val numListRDD = sc.parallelize(1 to 5, 2) //将数据分在2个分区里
    /*
      1、第一次调用Action操作时，持久化到内存和硬盘，
      2、防止每次Action都重复计算次transform，
      3、当不再用到此数据时，记得清除持久化的数据
     */
    numListRDD.persist(StorageLevel.MEMORY_ONLY)

    println(numListRDD.count()) //计数
    println(numListRDD.first())
    println(numListRDD.take(3).toBuffer) //获取源文件前面3个
    println(numListRDD.top(3).toBuffer) //将源文件倒叙后取前三（即，取最大的三个）

    println(numListRDD.sum()) //每个元素相加求和
    println(numListRDD.reduce(_ + _)) //定义前后两个元素的计算方式（这里为：前后求和）
    /*
      1、fold(n) 的执行原理：
         每个分区里进行这样的计算：初始值+sum(元素)
         最后进行：初始值+sum(分区 sum 值)
      2、即：初始值的相加次数=分区数+1
      3、fold(n)少用，一般在数据挖掘中使用
     */
    println(numListRDD.fold(10)(_ + _))
    numListRDD.unpersist() //必须手动清除持久化的数据，

    sc.stop()
  }

}
