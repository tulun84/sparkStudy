package zhibo8

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * 求Kmeans 中的K值
  */
object KMeans4Teams_K {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[8]").getOrCreate()
    // Load and parse the data
    val sc = ss.sparkContext

    val data = sc.textFile("C:\\Users\\cjp\\Desktop\\0330\\newTeams_dcast\\newTeams_dcast.csv").cache()
    data.take(2).foreach(println)
    val parsedData = data.filter(!_.contains("usercode"))
      .map(line => {
        Vectors.dense(line.split(",").drop(2).map(_.toDouble))
      }).cache()
    val sampleData = parsedData.sample(false, 0.7, 11L)
    println("数据总量：" + sampleData.count())
    println("数据示例：" + sampleData.take(10).foreach(println))
    //当然可以多跑几次，找一个稳定的 K 值。理论上 K 的值越大，聚类的 cost 越小，极限情况下，每个点都是一个聚类，
    // 这时候 cost 是 0，但是显然这不是一个具有实际意义的聚类结果
    val kCosts = Seq(30,40,50,60,70,80,85,90,95,100,105,110,120,130,200,250,300,400,500,600,700,800,900)
      .map { k =>
        (k, KMeans.train(parsedData, k, 5).computeCost(parsedData))
      }

    sc.stop()
    ss.stop()
  }
}