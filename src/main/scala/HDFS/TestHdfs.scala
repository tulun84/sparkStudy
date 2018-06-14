package HDFS

import org.apache.spark.{SparkConf, SparkContext}

object TestHdfs {
  def main(args: Array[String]): Unit = {
  /*  if(args.length<2){
      println("参数个数不对...")
      sys.exit(1)
    }*/
    val conf = new SparkConf().setAppName("TestHdfs").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val fileRDD=sc.textFile("hdfs://emr-header-1.cluster-61651:9000/user/hdfs/test/testTable_data.txt")
    val wc = fileRDD.filter(_.split(",").length>=4).flatMap(_.split(",")).map(x=>(x,1)).reduceByKey(_+_)
    wc.saveAsTextFile("hdfs://emr-header-1.cluster-61651:9000/user/hdfs/test/testTable_data_out")
    sc.stop()
  }
}
