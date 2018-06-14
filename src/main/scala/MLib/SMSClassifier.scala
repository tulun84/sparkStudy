package MLib

//import com.ibm.spark.exercise.util.LogUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, Word2Vec}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object SMSClassifier {
  final val VECTOR_SIZE = 100

  def main(args: Array[String]) {
    /*if (args.length < 1) {
    println("Usage:SMSClassifier SMSTextFile")
    sys.exit(1)
    }*/
    // LogUtils.setDefaultLogLevel()
    val filePath = "src/main/testFiles/SMSSpamCollection_Chinese.txt"
//    val filePath = "src/main/testFiles/Chinese_Split.txt"
    val conf = new SparkConf().setAppName("SMS Message Classification ( or SPAM)").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)
    val parsedRDD = sc.textFile(filePath)
      .map(_.trim.split("\t")).map(eachRow => {
      (eachRow(0), eachRow(1).split(" "))
    })

    parsedRDD.take(3).foreach(println)
    val msgDF = sqlCtx.createDataFrame(parsedRDD).toDF("label", "message")
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
      .setSeed(1234L)
      .setMaxIter(128)
      .setFeaturesCol("features")
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val Array(trainingData, testData) = msgDF.randomSplit(Array(0.8, 0.2))

    val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec, mlpc, labelConverter))
    val model = pipeline.fit(trainingData)

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
    sc.stop
  }
}