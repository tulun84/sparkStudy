package zhibo8

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * 加载 Kmeans 模型预测数据，并保存预测数据
  */
object KMeans4Teams_Predict {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[8]").getOrCreate()
    // Load and parse the data
    val sc = ss.sparkContext

    val data = sc.textFile("C:\\Users\\cjp\\Desktop\\0330\\newTeams_dcast\\newTeams_dcast.csv").cache()
    data.take(2).foreach(println) //查看数据格式

    val modelDir = "C:\\SparkMLib_Models/KMeansCluster4NewTeams"
    val model = KMeansModel.load(sc, modelDir)

    //预测并返回指定格式数据
    val dataPredicted = data
      .filter(!_.contains("usercode"))
      .map(line => {
        val arr = line.split(",")
        val label = arr(1)
        val dataArr = arr.drop(2).map(_.toDouble) //arr.drop(2) 数组丢弃前两个元素
        val predict = model.predict(Vectors.dense(dataArr))
        (label, predict, dataArr.mkString(","))
      }).cache()

    val predictedDir = "C:\\Users\\cjp\\Desktop\\0330\\newTeams_dcast\\newTeams_Kmeans"
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
    //保存预测数据
    dataPredicted.saveAsTextFile(predictedDir)
    sc.stop()
    ss.stop()
  }
}