package zhibo8

import java.io.File

import breeze.linalg.DenseVector
import com.alibaba.fastjson.JSON
import org.apache.commons.io.FileUtils

object test {
  def main(args: Array[String]): Unit = {
    val pattern = "[^\\u4e00-\\u9fa5a-zA-Z]".r
    val str11 = "Scala is scalable and cool #$23423432"

    println(pattern replaceAllIn (str11, " "))

    val predictedDir = "C:\\Users\\cjp\\Desktop\\newTeams_Kmeans"
    val predictedFile = new File(predictedDir)
    //如果文件夹存在则删除
    try {
      if (predictedFile.exists()) {
        FileUtils.deleteDirectory(predictedFile)
        println("目录已经存在，删除...")
      }
    } catch {
      case e => println(e)
    }



  val sss="([546150,骑士,凯尔特人)".substring(2,"([546150,骑士,凯尔特人)".length-1)
    print(sss)
    val list=List()
    list.::(1)
    list.::(2)
    list.::(1)
    list.foreach(println)
    val ss = "001 0.0 0.0 0.0"
    ss.split(" ").drop(1).foreach(println)
    val aa = DenseVector(1, 2, 3, 4)
    println(aa.toVector)
    var str01 = "123wer"
    println(str01.substring(1, str01.length))

    val str1 = "{'top_menu':2,'onlycare':1,'room_id':2}"
    println(str1)
    val json = JSON.parseObject(str1)

    val top_menu = json.get("top_menu")
    val onlycare = json.get("onlycare")
    val room_id = json.get("room_id")
    println(top_menu)

  }
}
