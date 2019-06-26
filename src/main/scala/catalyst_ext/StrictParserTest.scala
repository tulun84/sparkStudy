package catalyst_ext

import org.apache.spark.sql.SparkSession

/**
  * @Author: junping.chi@luckincoffee.com
  * @Date: 2019/6/26 15:08
  * @Description:
  */
object StrictParserTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .withExtensions(StrictParser.extBuilder)
      .getOrCreate()
    val dataFrame = spark.read.json("src/main/testFiles/people.json")
    dataFrame.createOrReplaceTempView("t_people")
    val sqlText = "select * from t_people"
    spark.sql(sqlText).show(false)

    spark.stop()
  }
}
