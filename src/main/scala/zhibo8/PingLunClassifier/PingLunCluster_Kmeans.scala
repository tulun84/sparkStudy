package zhibo8.PingLunClassifier

import java.util

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{MinMaxScaler, Word2Vec}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Spark ML 版本的KMeans聚类(本例中对文本聚类效果太差，放弃)
  */
object PingLunCluster_Kmeans {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local[8]").getOrCreate()
    // Load and parse the data
    val sc = ss.sparkContext
    val negPath = "C:\\公司数据\\zhibo8_pingLun\\words_neg_human_checked.csv"
    val posPath = "C:\\公司数据\\zhibo8_pingLun\\words_pos_human_checked.csv"
    //val fileRDD = sc.textFile("C:\\公司数据\\zhibo8_pingLun\\words_neg_human_checked.csv").cache()
    val negFileRDD = sc.textFile(negPath)
    val posFileRDD = sc.textFile(posPath).sample(withReplacement = false, 0.17, 110L)
    val count1 = negFileRDD.count()
    val count2 = posFileRDD.count()
    val fileRDD = negFileRDD.union(posFileRDD).cache()
    fileRDD.take(2)
    import ss.implicits._
    //先中文分词
    val filter = new StopRecognition()
    filter.insertStopNatures("w", "null") //过滤掉标点、空格
    filter.insertStopWords("的", "了", "我")
    filter.insertStopRegexes("[^\\u4e00-\\u9fa5a-zA-Z]") //正则：非中文且非英文
    val count3 = fileRDD.count()
    val wordsDF = fileRDD
      .filter(line => {
        line.trim.length > 0 && line.split(",").size == 2
      })
      .map(eachRow => {
        val rowArr = eachRow.trim.split(",")
        //中文分词
        val str = NlpAnalysis.parse(rowArr(0)).recognition(filter).toStringWithOutNature(" ")
        //返回格式：(类别标签，文本String[]数组)
        (rowArr(1), str.toString.split(" "))
      }).distinct() //distinct 去重，根据Key重新排序
      .toDF("label", "words")

    //转换为词向量
    val word2VecModel = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("vect")
      .setVectorSize(20)
      .setMinCount(5)//默认5
    val vect = word2VecModel.fit(wordsDF).transform(wordsDF)
    //词向量标准化
    val vect2 = new MinMaxScaler()
      .setInputCol("vect")
      .setOutputCol("features")
      .setMax(1.0)
      .setMin(0.0)
      .fit(vect)
      .transform(vect)
    //循环计算聚类个数的Cost值
    val costList = new util.ArrayList[Double]()
    for (k <- 2 to 15) {
      val kmeans = new KMeans()
        .setK(k)
        .setFeaturesCol("features")
        .setPredictionCol("prediction")
      val model = kmeans.fit(vect2)
      val d = model.computeCost(vect2)
      costList.add(d)
    }
    //打印个K值对应的Cost值
    import scala.collection.JavaConversions._
    for (c <- costList) {
      println(c)
     /* 28272.545163967516
      25991.32102099684
      23133.67681784112
      21622.536259670633
      20504.145617857674
      19905.19019196641
      18969.135275578294
      18635.28025510879
      18191.984303139085
      17610.22267783365
      17381.85859058989
      17044.167650204537
      16804.88614569756
      16389.41625010182*/
    }
    val kmeans = new KMeans()
      .setK(3)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
    val model = kmeans.fit(vect2)
    //保存模型
    model.write.overwrite().save("C:\\公司数据\\zhibo8_pingLun\\Models\\Kmeans_Model")
    //加载模型并预测保存数据
    val sameModel = KMeansModel.load("C:\\公司数据\\zhibo8_pingLun\\Models\\Kmeans_Model")
    val dataPredictedDF: DataFrame = sameModel.transform(vect2)
    dataPredictedDF.printSchema()
    val data4Save: DataFrame = dataPredictedDF.select("prediction","label","words")
    data4Save.rdd.saveAsTextFile("C:\\公司数据\\zhibo8_pingLun\\words_neg_human_checked_Kmeans.csv")
    ss.stop()
    sc.stop()
  }
}