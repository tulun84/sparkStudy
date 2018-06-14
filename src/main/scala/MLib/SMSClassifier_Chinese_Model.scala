package MLib

//import com.ibm.spark.exercise.util.LogUtils
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, Word2Vec}
import org.apache.spark.sql.SparkSession
/**
  * 短信分类器 多层感知器（前馈神经网络）识别短信分类
  * */
object SMSClassifier_Chinese_Model {
  final val VECTOR_SIZE = 100

  def main(args: Array[String]) {
    /*if (args.length < 1) {
    println("Usage:SMSClassifier SMSTextFile")
    sys.exit(1)
    }*/
    // LogUtils.setDefaultLogLevel()
    // val filePath = "src/main/testFiles/SMSSpamCollection_tulun.txt"
    val filePath = "src/main/testFiles/Chinese_Split.txt"
    val conf = new SparkConf().setAppName("SMS Message Classification ( or SPAM)").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val   sc=ss.sparkContext
    //先中文分词
    val filter = new StopRecognition()

    filter.insertStopNatures("w") //过滤掉标点

    val fileRDD = sc.textFile(filePath)
//    fileSplited.take(3).foreach(println)

    val parsedRDD = fileRDD
      .map(eachRow => {
        val rowArr = eachRow.trim.split("\\|")
        //中文分词
        val str = ToAnalysis.parse(rowArr(1)).recognition(filter).toStringWithOutNature(" ")
        //返回格式：(类别标签，文本String[]数组)
        (rowArr(0),str.toString.split(" "))
      })
    println(parsedRDD.count())
    parsedRDD.take(3).foreach(println)
    val msgDF = ss.createDataFrame(parsedRDD).toDF("label", "message").cache()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(msgDF)

    val word2Vec = new Word2Vec()
      .setInputCol("message")
      .setOutputCol("features")
      .setVectorSize(VECTOR_SIZE)
      .setMinCount(1)

    val layers = Array[Int](VECTOR_SIZE, 6, 5, 2)
    val mlpc = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(512)
      .setMaxIter(128)
      .setFeaturesCol("features")
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setSeed(1234L)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val Array(trainingData, testData) = msgDF.randomSplit(Array(0.8, 0.2))

    val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec, mlpc, labelConverter))
    val model = pipeline.fit(trainingData)
    model.write.overwrite().save("C:\\SparkMLib_Models\\Word2Vec_前馈神经_文本分类")//df.write.overwrite().save(path)可以覆盖已存在的outPutPath
    //model.save("C:\\SparkMLib_Models\\Word2Vec_前馈神经_文本分类")

    /**
      * 以下代码作为统计精确率
      */
    val predictionResultDF = model.transform(testData)
    //below 2 lines are for debug use
    predictionResultDF.printSchema
    predictionResultDF.select("message", "label", "predictedLabel").show(300)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      // .setMetricName("precision") // 这里的 precision 改成 accuracy 。不然会报错：parameter metricName given invalid value precision.
      .setMetricName("accuracy")
    val predictionAccuracy = evaluator.evaluate(predictionResultDF)
    println("Testing Accuracy is %2.4f".format(predictionAccuracy * 100) + "%")
    sc.stop()
    ss.stop()
  }
}