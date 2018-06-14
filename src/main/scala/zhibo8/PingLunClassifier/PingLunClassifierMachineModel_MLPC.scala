package zhibo8.PingLunClassifier

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, Word2Vec}
import org.apache.spark.sql.SparkSession

/**
  * 根据第三方机器识别语料建模
  */
object PingLunClassifierMachineModel_MLPC {
  final val VECTOR_SIZE = 1000

  def main(args: Array[String]) {
    /*if (args.length < 1) {
    println("Usage:SMSClassifier SMSTextFile")
    sys.exit(1)
    }*/
    // LogUtils.setDefaultLogLevel()
    // val filePath = "src/main/testFiles/SMSSpamCollection_tulun.txt"
    val negFilePath = "C:\\公司数据\\zhibo8_pingLun\\words_neg_machine_check.csv"
    val posFilePath = "C:\\公司数据\\zhibo8_pingLun\\words_pos_machine_check.csv"

    val conf = new SparkConf().setAppName("PingLunClassifierMachineModel").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext
    //sc.setLogLevel("WARN") //WARN DEBUG ERROR INFO
    //先中文分词
    val filter = new StopRecognition()
    filter.insertStopNatures("w") //过滤掉标点
    filter.insertStopWords("的", "了", "我")
    filter.insertStopRegexes("[^\\u4e00-\\u9fa5a-zA-Z]") //正则：非中文且非英文
    val negFileRDD = sc.textFile(negFilePath)
    val count1=negFileRDD.count()
    val posFileRDD = sc.textFile(posFilePath).sample(withReplacement = false,0.24,0L)
    val count2=posFileRDD.count()
    val fileRDD=negFileRDD.union(posFileRDD)
    val count3=fileRDD.count()
    //    fileSplited.take(3).foreach(println)
    fileRDD.takeSample(false,100,1L).foreach(println)

    val parsedRDD = fileRDD
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
    val msgDF = ss.createDataFrame(parsedRDD/*.take(1000000)*/).toDF("label", "message").cache()
    msgDF.createOrReplaceTempView("msgDF")
    val labels = ss.sql("select distinct(label) from msgDF").cache()
    println(msgDF.count())
    msgDF.show(10)
    val numOfLavel = labels.count()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(msgDF)

    val word2Vec = new Word2Vec()
      .setInputCol("message")
      .setOutputCol("features")
      .setVectorSize(VECTOR_SIZE)
      .setMinCount(1)
    /**
      * layers:这个参数是一个整型数组类型，第一个元素需要和特征向量的维度相等，最后一个元素需要训练数据的标签取值个数相等，
      * 如 2 分类问题就写 2(这里为四类：谩骂、广告、违禁、色情)。
      * 中间的元素有多少个就代表神经网络有多少个隐层，元素的取值代表了该层的神经元的个数。
      * 例如val layers = Array[Int](100,6,5,2)。
      */
    val layers = Array[Int](VECTOR_SIZE, 3, 2, labels.count().toInt) //
    val mlpc = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      // .setBlockSize(512)
      //.setMaxIter(128)
      .setFeaturesCol("features")
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setSeed(1234L)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val Array(trainingData, testData) = msgDF.randomSplit(Array(0.7, 0.3))

    val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec, mlpc, labelConverter))
    System.gc()
    val model = pipeline.fit(trainingData)
    model.write.overwrite().save("C:\\公司数据\\zhibo8_pingLun\\Models\\machineModel") //df.write.overwrite().save(path)可以覆盖已存在的outPutPath
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