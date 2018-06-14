package zhibo8

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by liuyanling on 2018/3/24   
  */
object KMeansCluster4NewTeams {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").getOrCreate()
    // Load and parse the data
    val sc = spark.sparkContext

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
    val kCosts = Seq(100, 120, 140, 160, 180, 200, 220, 250, 280, 320, 350, 400, 450, 500, 600, 700, 800, 1000)
      .map { k =>
        (k, KMeans.train(parsedData, k, 5).computeCost(parsedData))
      }
    val bw = new BufferedWriter(new FileWriter("C:\\Users\\cjp\\Desktop\\0330\\newTeams_dcast\\k_Costs.txt"))
    kCosts.foreach { case (k, cost) => {
      val s = s"WCSS for K=$k  cost=$cost"
      println(s)
      try {
        bw.write(s)
        bw.newLine
        bw.flush
      } catch {
        case e: Exception => println(e)
      }
    }
      //释放资源
      bw.close()
    }

    //释放资源
    /*
    10：1351599.158491371
    20：299417.6354752809
    30：131329.98468142218
    40：78672.23261984072
    50：45746.438699431834
    60：37056.93220933285
    70：23216.42368567548
    80：19640.408803936054
    90：17979.0815006626
    100：13006.015433235973
    120：8181.273865546572
    140：8412.0975292034
    160：5315.310087553346
    180：4791.966797196398
    200：3832.6075580119305

     */
    // Cluster the data into two classes using KMeans
    val numClusters = 200
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
    val sameModel = KMeansModel.load(sc, dir)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    val dataPredicted = data
      .filter(!_.contains("usercode"))
      .map(line => {
        val arr = line.split(",")
        val label = arr(1)
        val predict = sameModel.predict(Vectors.dense(arr.drop(2).map(_.toDouble)))
        (label, predict)
      })

    val predictedDir = "C:\\Users\\cjp\\Desktop\\0330\\newTeams_dcast\\newTeams_Kmeans"
    val file2 = new File(predictedDir)
    //如果文件夹存在则删除
    try {
      if (file2.exists()) {
        System.out.println("目录已经存在，删除...");
        FileUtils.deleteDirectory(file2)
      }
    } catch {
      case e => println(e)
    }

    dataPredicted.saveAsTextFile(predictedDir)
    sc.stop()
    spark.stop()
  }
}