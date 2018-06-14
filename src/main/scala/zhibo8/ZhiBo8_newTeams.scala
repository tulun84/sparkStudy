package zhibo8

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/**
  * usercode,name,type,league,count ,sum,count/sum ratio
  *
  */
object ZhiBo8_newTeams {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ZhiBo8_newTeams").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext
    import ss.implicits._
    val newTeamsRDD = ss.read.csv("C:\\Users\\cjp\\Desktop\\0330\\newTeams")
      .cache()
      .rdd
      .filter(row => {
        row.size > 0
      })
      .map(row => {
        var usercode = row.getString(0)
        val name = row.getString(1)
        val type_ = row.getString(2)
        var league = row.getString(3)
        if (usercode.startsWith("[")) {
          usercode = usercode.substring(1, usercode.length)
          league = league.substring(0, league.length - 1)
        }
        (usercode, name, type_, league)
      })
      .toDF("usercode", "name", "type", "league")
      .cache()
      .createOrReplaceTempView("new_teams")
    val sql =
      s"""
         |select t1.*,t2.sum,t1.count/t2.sum ratio from
         |(
         |(select usercode,name,type,league,count(1) count
         |from new_teams group by  usercode,name,type,league
         |) t1 left join
         |(select usercode,count(1) sum from new_teams group by usercode
         |) t2
         |on t1.usercode=t2.usercode
         |)
       """.stripMargin
    /*  val sql =
        s"""select * from new_teams where usercode like '%[%' limit 10"""*/

    //    val result = ss.sql(sql).rdd.foreach(println)
    val result = ss.sql(sql).cache()
    result.show()
    result.rdd.repartition(1).saveAsTextFile("C:\\Users\\cjp\\Desktop\\0330\\new_teams02.csv")
    //    result.printSchema() //打印表结构
    //
    //    fileRDD.unpersist()
    sc.stop()
    ss.stop()
  }


}