package MLib.feature

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * RFormula 特征选择
  * 博客：https://www.cnblogs.com/xing901022/p/7152922.html
  **/
object RFormulaTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RFormula-Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    var sqlContext = new SQLContext(sc)

    val dataset = sqlContext.createDataFrame(Seq(
      (7, "US", "回家", 1.0, "a"),
      (8, "CA", "上学", 0.0, "b"),
      (9, "NZ", "打游戏", 0.0, "a")
    )).toDF("id", "country", "hour", "clicked", "my_test")
    val formula = new RFormula()
      .setFormula("clicked ~ country + hour + my_test") //
      .setFeaturesCol("features")
      .setLabelCol("label")
    val output = formula.fit(dataset).transform(dataset)
    output.show()
    output.select("features", "label").show()
  }
}