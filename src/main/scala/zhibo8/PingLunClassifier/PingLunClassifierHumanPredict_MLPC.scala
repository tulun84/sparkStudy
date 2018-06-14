package zhibo8.PingLunClassifier

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession

/**
  * 根据第三方机器识别语料建模
  */
object PingLunClassifierHumanPredict_MLPC {
  final val VECTOR_SIZE = 20

  def main(args: Array[String]) {
    /*if (args.length < 1) {
    println("Usage:SMSClassifier SMSTextFile")
    sys.exit(1)
    }*/
    // LogUtils.setDefaultLogLevel()
    // val filePath = "src/main/testFiles/SMSSpamCollection_tulun.txt"
    val negPath = "C:\\公司数据\\zhibo8_pingLun\\words_neg_machine_check.csv"
    val posPath = "C:\\公司数据\\zhibo8_pingLun\\words_pos_machine_check.csv"
    val conf = new SparkConf().setAppName("PingLunClassifierHumanPredict_MLPC").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext
    //sc.setLogLevel("DEBUG") //WARN DEBUG ERROR INFO
    //先中文分词
    val filter = new StopRecognition()
    filter.insertStopNatures("w", "null") //过滤掉标点、空格
    //filter.insertStopWords("的", "了", "我")
    //filter.insertStopRegexes("[^\\u4e00-\\u9fa5a-zA-Z]") //正则：非中文且非英文
    val negFileRDD = sc.textFile(negPath).sample(withReplacement = false, 0.002, 110L)
    val posFileRDD = sc.textFile(posPath).sample(withReplacement = false, 0.0005, 212L)
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
      .cache()

    val msgDF = ss.createDataFrame(parsedRDD).toDF("label", "message").cache()
    msgDF.createOrReplaceTempView("msgDF")
    println(msgDF.count())
    msgDF.show(100)

    val sameModel = PipelineModel.load("C:\\公司数据\\zhibo8_pingLun\\Models\\humanModel")

    val predictionResultDF = sameModel.transform(msgDF).sort("predictedLabel", "label")

    predictionResultDF.select("predictedLabel", "label", "message").rdd.repartition(1).saveAsTextFile("C:\\公司数据\\zhibo8_pingLun\\Models\\predictMachein_ByHumanModel")
    predictionResultDF.select("predictedLabel", "label", "message").show(30)
    //


    sc.stop()
    ss.stop()
  }
}