package MLib

//import com.ibm.spark.exercise.util.LogUtils
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession

object SMSClassifier_Chinese_predict {

  def main(args: Array[String]) {
    /*if (args.length < 1) {
    println("Usage:SMSClassifier SMSTextFile")
    sys.exit(1)
    }*/
    // LogUtils.setDefaultLogLevel()
    val filePath = "src/main/testFiles/Chinese_Split.txt"
    val conf = new SparkConf().setAppName("ZhiBo8_newTeams").setMaster("local[8]")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = ss.sparkContext

    //先中文分词
    val filter = new StopRecognition()
    filter.insertStopNatures("w") //过滤掉标点

    val fileSplited = sc.textFile(filePath)
//    fileSplited.take(3).foreach(println)
import ss.implicits._
    val parsedRDD = fileSplited
      .map(eachRow => {
        val rowArr = eachRow.trim.split("\\|")
        val str = ToAnalysis.parse(rowArr(1)).recognition(filter).toStringWithOutNature(" ")
        (str.toString.split(" "))
      })

    val msgDF=parsedRDD.toDF("message")
//    println(parsedRDD.count())
//    parsedRDD.take(3).foreach(println)
//    val msgDF = sqlCtx.createDataFrame(parsedRDD).toDF("label", "message")

    val sameModel = PipelineModel.load("C:\\SparkMLib_Models\\Word2Vec_前馈神经_文本分类")
    val predictionResultDF = sameModel.transform(msgDF)
    //below 2 lines are for debug use
    predictionResultDF.printSchema
    predictionResultDF.select("message", "predictedLabel").show(300)

    sc.stop
    ss.stop()
  }
}