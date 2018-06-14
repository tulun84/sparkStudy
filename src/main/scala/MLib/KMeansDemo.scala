package MLib

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
/**
  * Created by liuyanling on 2018/3/24   
  */
object KMeansDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val df = spark.read.format("libsvm").load("src/main/testFiles/kmeans_data.txt")
    //setK设置要分为几个类 setSeed设置随机种子
    val kmeans = new KMeans().setK(4).setSeed(1L)
    //聚类模型
    val model = kmeans.fit(df)
    // 预测 即分配聚类中心
    model.transform(df).show(false)
    //聚类中心
    model.clusterCenters.foreach(println)
    // SSE误差平方和
    println("SSE:"+model.computeCost(df))
  }
}