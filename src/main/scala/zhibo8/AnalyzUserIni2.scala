package zhibo8

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * useini表:
  * "id","udid","usercode","platform","name","val","createtime","os"
  * key:usercode,计算每个用户的“视频默认显示录像”、是否显示弹幕、获取比分推送的百分比
  */
object AnalyzUserIni2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AnalyzUserConf").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext


    val fileRDD = ss.read.csv("C:\\Users\\cjp\\Desktop\\0330\\user_ini02.csv\\user_ini02.csv")
    fileRDD.cache()
      .toDF("id", "udid", "usercode", "platform", "name", "vall", "createtime", "os")
      .createOrReplaceTempView("userconf")
    val sql =
      s"""
         |select *,c1.count/c2.sum ratio,c1.count/c2.sum ratio2
         |from (
         |(select usercode,name,vall,count(1) count
         |from userconf
         |group by usercode,name,vall
         |) c1 left join
         |(select usercode,count(1) sum
         |from userconf  group by usercode
         |) c2
         |on c1.usercode=c2.usercode
         |)
         |""".stripMargin

    val result = ss.sql(sql).cache()
    /* val rdd = result.rdd.map(row => {
       val usercode = row.get(1)
       val name = row.get(2)
       val vall = row.get(3)
       val count = row.get(4)
       val sum = row.get(5)
       val ratio = row.getDouble(6)
       val nameVall = String.valueOf(name) + String.valueOf(vall)
       if (nameVall.contains("弹幕") && nameVall.contains("开启")) {
         val barrageRatio = ratio //给个user开启弹幕的次数比例
       } else if (nameVall.contains("录像") && nameVall.contains("关")) {
         val barrageRatio = 1 - ratio //每个user开启录像的次数比例
       } else if (nameVall.contains("比分") && nameVall.contains("开")) {
         val barrageRatio = ratio //每个user开启录像的次数比例
       }

       (usercode, String.valueOf(name) + String.valueOf(vall), count, sum, ratio)
     })*/


    result.rdd.repartition(1).saveAsTextFile("C:\\Users\\cjp\\Desktop\\0330\\user_ini03_aggr.csv")


    ss.stop()
    sc.stop()
  }
}
