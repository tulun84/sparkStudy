package zhibo8

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ZhiBo8_UserConf {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ZhiBo8_UserConf").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext
    /*导入隐式转换*/
    /*获取源数据*/

    import ss.implicits._
    val fileRDD = ss.read.csv("C:\\Users\\cjp\\Desktop\\0330\\user_conf.csv")
      .cache() //在读取数据之后马上就要cache,之后的RDD进行cache不会起作用
      .rdd
      .filter(line => line(4).toString != "conf_str") //将空行过滤掉
      .map(line => {
      val id = String.valueOf(line(0))
      val udid = String.valueOf(line(1))
      val usercode = String.valueOf(line(2))
      val platform = String.valueOf(line(3))
      var conf_str = String.valueOf(line(4)) + "," + String.valueOf(line(5)) + "," + String.valueOf(line(6))
      conf_str = conf_str.replaceAll("\"\"", "\"")
      conf_str = conf_str.substring(1, conf_str.indexOf("}") + 1)
      //      val createtime = line(5)
      //      val updatetime = line(6)
      //      val os = line(7)
      //      println("str1: " + str1)

      if ((!conf_str.startsWith("{")) || (!conf_str.endsWith("}"))) conf_str = "{}"
      val json = JSON.parseObject(conf_str)
      val top_menu = String.valueOf(json.get("top_menu"))
      val onlycare = String.valueOf(json.get("onlycare"))
      val room_id = String.valueOf(json.get("room_id"))
      (id.toString, udid, usercode, platform, top_menu, onlycare, room_id, room_id) //返回三个元素的元组：Tuple3对象
    }).toDF("id", "udid", "usercode", "platform", "top_menu", "onlycare", "room_id", "room_id2")
      .cache()
     // .createOrReplaceTempView("userconf")

/*val sql=s"""select usercode,top_menu from userconf group by usercode,top_menu """
    ss.sql()*/
     fileRDD.rdd.repartition(1).saveAsTextFile("C:\\Users\\cjp\\Desktop\\0330\\user_conf_02.csv")


    sc.stop()
    ss.stop()
  }

}