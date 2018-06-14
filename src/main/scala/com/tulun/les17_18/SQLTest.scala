package com.tulun.les17_18

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sparkSession 练习
  * 1.SparkSession  的数据源来自哪里？
  *   1.1 程序内部，把 RDD 映射为临时数据表（视图），用 sql 进行查询。
  * case class 可以隐式转为 DataSet
  *   1.2 读外部数据，比如 HDFS、Hive、Json、Parquet 文件等。
  *
  */
case class Log(id: Int, content: String) //定义一个Log的case class类
object SQLTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SQLTest").setMaster("local")
    val ss = SparkSession.builder()
    //.master("local")
    //.appName("SQLTest2")
    .config(conf) // 从sparkConf中获取配置参数
    .getOrCreate()

    import ss.implicits._

    /*从sparkSession中获取sparkContext对象*/
    val sc = ss.sparkContext
    // 模拟一个RDD
    //    val fileRDD = sc.parallelize(1 to 90).map(i => (i, s"content_$i"))
    //    fileRDD.toDF("id", "content").createOrReplaceTempView("LogTable")
    // ss创建DF的方式
    val fileRDD = ss.createDataFrame((1 to 90).map(i => Log(i, s"content_$i")))
    //.toDF("rowid", "myContent") //可以自己定义字段名，否则默认为Log2类的字段名
    fileRDD.createOrReplaceTempView("LogTable")
    val top=15
    val down=10
    val sql=
      s""" select * from LogTable
         | where id >=$down
         | and id <=$top
       """.stripMargin

    val result = ss.sql(sql)
    result.show() //预览查询结果
    result.printSchema() //打印表结构
    result.rdd.map(row => {
      // val id = row.getInt(0)
      // val content = row.getString(1)
      val id = row.getAs[Int]("id")
      val content = row.getAs[String]("content")
      (id, content)
    }).foreach(println)

    println("=======================================")

    /*写法2：*/
    result.rdd.map {
      case Row(rowid: Int, myContent: String) =>
        (s"$rowid", s"$myContent")
    }.foreach(println)

    sc.stop()
    ss.stop()
  }

}
