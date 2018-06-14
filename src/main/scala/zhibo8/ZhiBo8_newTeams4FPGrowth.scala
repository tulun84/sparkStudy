package zhibo8

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * usercode,name,type,league,count ,sum,count/sum ratio
  *
  */
object ZhiBo8_newTeams4FPGrowth {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ZhiBo8_newTeams").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

       val sc = ss.sparkContext


    val newTeamsRDD = sc.textFile("C:\\Users\\cjp\\Desktop\\0330\\new_teams02.csv")
      .map(line => {
        val arr = line.split(",")
        val usercode = arr(0)
        val teamName = arr(1)
        (usercode, teamName)
      }).reduceByKey(_+","+_)
      .cache()
    newTeamsRDD.take(2).foreach(println)

    newTeamsRDD.saveAsTextFile("C:\\Users\\cjp\\Desktop\\0330\\new_teams_userAndTeams.csv")
    sc.stop()
    ss.stop()
  }


}