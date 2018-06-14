package com.tulun.les17_18

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * sparkSession 练习
  * 1.SparkSession  的数据源来自哪里？
  *   1.1 程序内部，把 RDD 映射为临时数据表（视图），用 sql 进行查询。
  * case class 可以隐式转为 DataSet
  *   1.2 读外部数据，比如 HDFS、Hive、Json、Parquet 文件等。
  *
  */
case class Log2(id: Int, content: String) //定义一个Log的case class类
object SQLTest2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("").setMaster("local")
    val ss = SparkSession.builder()
      //      .master("local")
      //      .appName("SQLTest2")
      .config(conf) // 从sparkConf中获取配置参数
      .getOrCreate()

    val fileRDD = ss.createDataFrame((1 to 90).map(i => Log2(i, s"content_$i")))
      .toDF("rowid","myContent") //可以自己定义字段名，否则默认为Log2类的字段名
    fileRDD.toDF("id", "content").createOrReplaceTempView("LogTable")
    val result = ss.sql("select * from LogTable")

  }

}
