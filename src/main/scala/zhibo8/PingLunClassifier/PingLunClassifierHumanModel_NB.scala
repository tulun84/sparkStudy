package zhibo8.PingLunClassifier

import java.util

import org.ansj.recognition.impl.StopRecognition
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apdplat.word.WordSegmenter
import org.apdplat.word.segmentation.{SegmentationAlgorithm, Word}

/**
  * 根据第三方机器识别语料建模
  */
object PingLunClassifierHumanModel_NB {
  final val VECTOR_SIZE = 10

  def main(args: Array[String]) {
    /*if (args.length < 1) {
    println("Usage:SMSClassifier SMSTextFile")
    sys.exit(1)
    }*/
    // val filePath = "src/main/testFiles/SMSSpamCollection_tulun.txt"
    val negPath = "C:\\公司数据\\zhibo8_pingLun\\words_neg_human_checked.csv"
    val posPath = "C:\\公司数据\\zhibo8_pingLun\\words_pos_human_checked.csv"
    val conf = new SparkConf().setAppName("PingLunClassifierHumanModel_NB").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext
    sc.setLogLevel("WARN") //WARN DEBUG ERROR INFO
    //先中文分词
    val filter = new StopRecognition()
    filter.insertStopNatures("w", "null") //过滤掉标点、空格
    // filter.insertStopWords("的", "了", "我")
    // filter.insertStopRegexes("[^\\u4e00-\\u9fa5a-zA-Z]") //正则：非中文且非英文
    val negFileRDD = sc.textFile(negPath).sample(withReplacement = false, 0.1, 110L)
    val posFileRDD = sc.textFile(posPath).sample(withReplacement = false, 0.017, 118L)
    val count1 = negFileRDD.count()
    val count2 = posFileRDD.count()
    val fileRDD = negFileRDD.union(posFileRDD)
    val count3 = fileRDD.count()

    fileRDD.take(100).foreach(println)
    val parsedRDD = fileRDD
      .filter(line => {
        line.trim.length > 0 && line.split("\",\"").size == 2
      })
      .map(eachRow => {
        val rowArr = eachRow.trim.split("\",\"")
        var label: String = rowArr(1)

        if (label == "positive\"") {
          label = 1 + ""
        } else {
          label = 0 + ""
        }
        //中文分词
        //val lineNLP = NlpAnalysis.parse(rowArr(0)).recognition(filter).toStringWithOutNature(" ")
        val wordList: util.List[Word] = WordSegmenter.segWithStopWords(rowArr(0), SegmentationAlgorithm.MaxNgramScore)
        if (wordList != null && wordList.size() > 0) {

          //同义标注,false表示启用间接同义
          //SynonymTagging.process(wordList /*, false*/)
          //做反义标注：
          //AntonymTagging.process(wordList)
          //执行拼音标注：
         // PinyinTagging.process(wordList)
        }
        //返回格式：(类别标签，文本String[]数组)
        //(rowArr(1), str.toString.split(" "))
        (label.toDouble, wordList.toString)
      }).distinct() //distinct 去重，根据Key重新排序
      .cache()

    parsedRDD.take(100).foreach(println)
    val msgDF = ss.createDataFrame(parsedRDD).toDF("label", "message").cache()
    val Array(trainingData, testData) = msgDF.randomSplit(Array(0.8, 0.2))

    msgDF.createOrReplaceTempView("msgDF")
    val labels = ss.sql("select distinct(label) from msgDF")
    println(msgDF.count())
    msgDF.show(100)
    println(labels.count())


    // step 1 sentence 拆成 words
    val tokenizer = new RegexTokenizer().setInputCol("message").setOutputCol("words").setPattern(" ")
    // step 2 label 转化为以0开始的labelIndex 为了适应spark.ml
    val indexer = new StringIndexer().setInputCol("label").setOutputCol("labelIndex").fit(msgDF)
    // step3 统计tf词频
    val countModel = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures")
    // step4 tf-idf
    val idfModel = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // step5 normalize tf-idf vector
    val normalizer = new Normalizer().setInputCol("features").setOutputCol("normalizedFeatures")
    // step6 naive bayes model
    val naiveBayes = new NaiveBayes().setFeaturesCol("normalizedFeatures").setLabelCol("labelIndex") /*.setWeightCol("obsWeights")*/ .setPredictionCol("prediction").setModelType("multinomial").setSmoothing(1.0)
    // step7 predict label to real label
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(indexer.labels)

    val pipeline: Pipeline = new Pipeline().setStages(Array(tokenizer, indexer, countModel, idfModel, normalizer, naiveBayes, labelConverter))
    // train and test
    val combinedModel = pipeline.fit(trainingData)

    val predictResult = combinedModel.transform(testData)
    predictResult.createTempView("predictResult")
    val rs1: DataFrame = ss.sql("select count(1) from predictResult where predictedLabel=label")
    val rs2: DataFrame = ss.sql("select count(1) from predictResult")
    rs1.show()
    rs2.show()

    val value: RDD[(Double, Double)] = predictResult.select("predictedLabel", "label").rdd.map(row => (row.getString(0).toDouble, row.getDouble(1)))
    val evaluator = new MulticlassMetrics(value)

    println("confusionMatrix:")
    println(evaluator.confusionMatrix)
    println(evaluator.accuracy)
    sc.stop()
    ss.stop()
  }
}