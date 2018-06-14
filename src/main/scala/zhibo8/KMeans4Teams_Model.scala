package zhibo8

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * 根据 K 值训练 Kmwans 模型并保存
  */
object KMeans4Teams_Model {
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

    // Cluster the data into two classes using KMeans
    val numClusters = 10
    val numIterations = 20
    //训练次数
    val start = System.currentTimeMillis()
    //val model = KMeans.train(sampleData, numClusters, numIterations)
    val initMode = "k-means||" //设置初始K选取方式为k-means++
    val model = new KMeans().setInitializationMode(initMode)
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .setSeed(11L)
      .run(parsedData)
    val end = System.currentTimeMillis()
    println("Time of caculateting Model: " + (end - start))
    //查看Model参数
    model.clusterCenters.foreach(println)
    println(model.k)

    // Save and load model
    val dir = "C:\\SparkMLib_Models/KMeansCluster4NewTeams"
    val file = new File(dir)
    //如果文件夹存在则删除
    try {
      if (file.exists()) {
        System.out.println("目录已经存在，删除...");
        FileUtils.deleteDirectory(file)
      }
    } catch {
      case e => println(e)
    }

    model.save(sc, dir)

    sc.stop()
    ss.stop()
  }
}