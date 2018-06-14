package zhibo8.PingLunClassifier

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, Word2Vec}
import org.apache.spark.sql.SparkSession

/**
  * 根据第三方机器识别语料建模
  */
object PingLunClassifierHumanModel_RandomForest {
  final val VECTOR_SIZE = 30

  def main(args: Array[String]) {
    /*if (args.length < 1) {
    println("Usage:SMSClassifier SMSTextFile")
    sys.exit(1)
    }*/
    // LogUtils.setDefaultLogLevel()
    // val filePath = "src/main/testFiles/SMSSpamCollection_tulun.txt"
    val negPath = "C:\\公司数据\\zhibo8_pingLun\\words_neg_human_checked.csv"
    val posPath = "C:\\公司数据\\zhibo8_pingLun\\words_pos_human_checked.csv"
    val conf = new SparkConf().setAppName("PingLunClassifierHumanModel").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext
    //sc.setLogLevel("DEBUG") //WARN DEBUG ERROR INFO
    //先中文分词
    val filter = new StopRecognition()
    filter.insertStopNatures("w", "null") //过滤掉标点、空格
    filter.insertStopWords("的", "了", "我")
    filter.insertStopRegexes("[^\\u4e00-\\u9fa5a-zA-Z]") //正则：非中文且非英文
    val negFileRDD = sc.textFile(negPath)
    val posFileRDD = sc.textFile(posPath).sample(withReplacement = false, 0.17, 110L)
    val count1 = negFileRDD.count()
    val count2 = posFileRDD.count()
    val fileRDD = negFileRDD.union(posFileRDD)
    val count3 = fileRDD.count()
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
    val msgDF = ss.createDataFrame(parsedRDD /*.take(120000)*/).toDF("label", "message").cache()
    val Array(trainingData, testData) = msgDF.randomSplit(Array(0.8, 0.2))

    msgDF.createOrReplaceTempView("msgDF")
    val labels = ss.sql("select distinct(label) from msgDF")
    println(msgDF.count())
    msgDF.show(100)
    println(labels.count())


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

    val layers = Array[Int](VECTOR_SIZE, 6, 5, labels.count().toInt) //

    //这里定义算法，可分别测试随机森林、朴素贝叶斯、mlpc(前馈神经网络)、GBDT等分类算法
    val rfClassifier = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
      .setNumTrees(20)

    new GaussianMixture
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec, rfClassifier, labelConverter))
    val model = pipeline.fit(trainingData)
    model.write.overwrite().save("C:\\公司数据\\zhibo8_pingLun\\Models\\humanModel") //df.write.overwrite().save(path)可以覆盖已存在的outPutPath
    //model.save("C:\\SparkMLib_Models\\Word2Vec_前馈神经_文本分类")

    /**
      * 以下代码作为统计精确率
      */
    val predictionResultDF = model.transform(testData)

    //below 2 lines are for debug use
    predictionResultDF.printSchema
    val resutDF = predictionResultDF.select("message", "label", "predictedLabel").toDF("message", "label", "predictedLabel").createOrReplaceTempView("preDataTable")
    val rs1 = ss.sql("select count(1) num from preDataTable where label=predictedLabel")
    val rs2 = ss.sql("select count(1) sum from preDataTable")
    rs1.show()
    rs2.show()
    ss.sql("select *  from preDataTable").show(1000)

    sc.stop()
    ss.stop()
  }
}