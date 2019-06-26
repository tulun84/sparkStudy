import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {


  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .appName("Testaa")
      .master("local[8]")
      /*spark core配置*/
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //启用序列化
      .config("spark.default.parallelism", "40") //设置默认并行度（num-executor*executor-core的2-3倍）
      .getOrCreate()

    /*设置spark 日志级别*/
    // ss.sparkContext.setLogLevel("WARN")
    val frame: DataFrame = ss.createDataFrame(Seq(
      ("1班", null, null, 10100),
      ("1班", "语文", null, 1020),
      ("1班", "语文", "jack03", 100),
      ("1班", "数学", "jack01", 2001),
      ("1班", "数学", "jack02", 232880),
      ("1班", "数学", "jack03", 2310),
      ("2班", null, "jack01", null),
      ("2班", null, "jack02", null),
      ("2班", null, "jack03", 128800),
      ("2班", "数学", "jack01", 22001),
      ("2班", "数学", "jack02", 2288320),
      ("2班", "数学", "jack03", 22310)
    )).toDF("class", "xueke", "name", "score")
    frame.createOrReplaceTempView("tmp")
    /*2、 求各个班级，各个学科，的最高分和对应学生名字,同时返回分数top5*/
    val sql =
      s"""
         |select * from tmp
       """.stripMargin
    ss.sql(sql).show(false)

  }
}