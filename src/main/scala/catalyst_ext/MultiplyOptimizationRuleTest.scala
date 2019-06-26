package catalyst_ext

import org.apache.spark.sql.SparkSession

object MultiplyOptimizationRuleTest {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .withExtensions(MultiplyOptimizationRule.extBuilder)
      .getOrCreate()

    val dataFrame = spark.read.json("src/main/testFiles/people.json")
    dataFrame.createOrReplaceTempView("t_people")
    val df = spark.sql("select age*1  from t_people")
    df.show()
    val numberedTreeString = df.queryExecution.optimizedPlan.numberedTreeString
    println("============================")
    println(numberedTreeString)
    println("============================")
    // scalastyle:on
    spark.stop
  }
}
