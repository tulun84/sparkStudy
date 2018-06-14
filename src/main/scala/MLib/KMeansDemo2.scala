package MLib

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by liuyanling on 2018/3/24   
  */
object KMeansDemo2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    // Load and parse the data
    val sc = spark.sparkContext

    val data = sc.textFile("src/main/testFiles/kmeans_data02.txt")
    val parsedData = data.map(line => {
      Vectors.dense(line.split(" ").drop(1).map(_.toDouble))
    }).cache()
    // Cluster the data into two classes using KMeans

    val kCosts = Seq(3, 4, 5, 6, 7, 8, 9).map { k =>
      (k, KMeans.train(parsedData, k, 5).computeCost(parsedData)) }
    kCosts.foreach { case (k, cost) => println(s"WCSS for K=$k id $cost") }

    val numClusters = 2
    val numIterations = 20
    val model = KMeans.train(parsedData, numClusters, numIterations)
    // Save and load model
    model.save(sc, "C:\\SparkMLib_Models/KMeansModel")
    val sameModel = KMeansModel.load(sc, "C:\\SparkMLib_Models/KMeansModel")


    val parsedDataWithCluster = model.predict(parsedData)
    parsedDataWithCluster.foreach(println)
    model.clusterCenters.foreach(println)
    println(model.k)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    data.map(line => {
      val arr = line.split(" ")
      val label = arr(0)
      val predict = sameModel.predict(Vectors.dense(arr.drop(1).map(_.toDouble)))
      (label, predict)
    }).foreach(println)

  }
}