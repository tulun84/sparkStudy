package zhibo8

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * useini表:
  * "id","udid","usercode","platform","name","val","createtime","os"
  * key:usercode,计算每个用户的“视频默认显示录像”、是否显示弹幕、获取比分推送的百分比
  */
object AnalyzUserIni {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AnalyzUserConf").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext

    val fileRDD = sc.textFile("C:\\Users\\cjp\\Desktop\\0330\\user_ini02.csv\\user_ini02.csv")
      .cache()
    val reduceChildRDD = fileRDD.map(line => {
      val arr = line.split(",")
      val usercode = arr(2)
      val name = arr(4)
      val vall = arr(5)
      ((usercode, name, vall), 1)
    }).reduceByKey(_ + _)
      .map(t => {
        val usercode = t._1._1
        val name = t._1._2
        val vall = t._1._3
        val count = t._2
        (usercode, (name, vall, count))
      })

    val reduceRDD = fileRDD.map(line => {
      val arr = line.split(",")
      val usercode = arr(2)
      (usercode, 1)
    }).reduceByKey(_ + _)
    reduceChildRDD.leftOuterJoin(reduceRDD)
      .map(t=>{
        val usercode = t._1
        val name = t._2._1._1
        val vall =t._2._1._2
        val count =t._2._1._3
        val sum= t._2._2.getOrElse(0)
        (usercode,name,vall,count,sum,count/sum)
      }).sortBy(_._6,false).top(1000).foreach(println)
    ss.stop()
    sc.stop()
  }
}
