package zhibo8

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ZhiBo8_UserIni {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ZhiBo8_UserIni").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext
    /*导入隐式转换*/
    /*获取源数据*/
   /* val fileRDD = sc.textFile("C:\\Users\\cjp\\Desktop\\0330\\user_ini.csv")
      .cache() //在读取数据之后马上就要cache,之后的RDD进行cache不会起作用
      .take(1000)
      .filter(line => line.length > 0) //将空行过滤掉
      .map(line => {
      val arr = line.split(",") //hive默认分隔符“\001”
      val id = arr(0) //trackTime的时间格式 2001-01-01 22:22:22
      val udid = arr(1) //用户id
      val usercode = arr(2) //用户id
      val platform = arr(3) //url
      val name = arr(4) //url
      val vall = arr(5) //url
      val createtime = arr(6) //url
      val os = arr(7) //url
      (id, udid, usercode, platform, name, vall, createtime, os) //返回三个元素的元组：Tuple3对象
    }) //.filter(line => line._5 == "视频默认显示录像" /*|| line._5 == "是否显示弹幕" || line._5 == "获取比分推送"*/) //将空行过滤掉
      .take(1000).toSeq
      .toDF("id", "udid", "usercode", "platform", "name", "vall", "createtime", "os")
      .persist(StorageLevel.MEMORY_ONLY) //初步整理完后，进行持久化

    //    fileRDD.take(3).foreach(print)
    /*创建临时视图*/
    fileRDD.createOrReplaceTempView("userini")
    /*构建sql语句*/

    val sql =
      s"""select udid,usercode,name,vall
         |from userini
         |group by udid,usercode,name,vall limit 100""".stripMargin
    val result = ss.sql(sql).cache()
    result.show() //预览查询结果
    val rdd = result.rdd
//    val value = rdd.coalesce(1, true)
    rdd.take(10).foreach(println)*/

    ss.read.csv("C:\\Users\\cjp\\Desktop\\0330\\user_ini.csv")
      .toDF("id","udid","usercode","platform","name","val","createtime","os")
      .createOrReplaceTempView("user_ini")
    val result = ss.sql("select * from user_ini where name in(\"视频默认显示录像\",\"是否显示弹幕\",\"获取比分推送\")").cache()
    result.show()
    result.rdd.repartition(1).saveAsTextFile("C:\\Users\\cjp\\Desktop\\0330\\user_ini02.csv")
//    result.printSchema() //打印表结构
//
//    fileRDD.unpersist()
    sc.stop()
    ss.stop()
  }

}