package zhibo8

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ZhiBo8_Kmeans {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ZhiBo8_Kmeans").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc=ss.sparkContext
    import ss.implicits._

    val fileRDD=sc.textFile("C:\\Users\\cjp\\Desktop\\0330\\newTeams_dcast\\newTeams_dcast.csv")
      .toDF()
      .createOrReplaceTempView("user_ini_dcast")



    //      .createOrReplaceTempView("user_ini_dcast")

    /* ss.read.csv("C:\\Users\\cjp\\Desktop\\0330\\user_ini.csv")
       .toDF("id","udid","usercode","platform","name","val","createtime","os")
       .createOrReplaceTempView("user_ini")

     ss.read.csv("C:\\Users\\cjp\\Desktop\\0330\\user_ini.csv")
       .toDF("id","udid","usercode","platform","name","val","createtime","os")
       .createOrReplaceTempView("user_ini")
 */
    val result = ss.sql("select * from user_ini_dcast limit 10")
    result.show()

    sc.stop()
    ss.stop()
  }

}