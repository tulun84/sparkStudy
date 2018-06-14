package zhibo8

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * FPGrowth 关联规则
  */
object FPGrowth4Teams {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FP_Growth").setMaster("local[8]")
    val sc = new SparkContext(conf)

    // ([546150,骑士,凯尔特人)
    val data = sc.textFile("C:/Users/cjp/Desktop/0330/new_teams_userAndTeams.csv/").cache()
    //val data = sc.textFile("src/main/testFiles/sample_fpgrowth_zhibo8.txt").cache()
    val transactions: RDD[Array[String]] = data.map(line => line.trim.substring(2, line.length - 1).split(",").drop(1))

//    val data = sc.textFile("src/main/testFiles/sample_fpgrowth_label.txt").cache()
//    val transactions: RDD[Array[String]] = data.map(line => line.trim.split(" ").drop(1))
    /**
      * 假如支持度：3%，置信度：40%
      * 支持度3%：意味着3%顾客 同时 购买牛奶和面包（同时发生的概率）
      * 置信度40%：意味着购买牛奶的顾客40%也购买面包（A发生=>B发生的概率）
      */
    transactions.take(3).foreach(s=>println(s.mkString("",",","")))
    val minSupport = 0.2 //支持度,满足支持度阈值0.2的组合作为“频繁项集”
    val minConfidence = 0.6 //置信度

    // 每行数据中的元素不能重复，调研array的distinct()方法去重
    val unique = transactions.map(x=>x.distinct).cache()
    println("AAAAAAAAAAAAAAAAAAAA"+unique.count())
    unique.take(10).foreach(arr=>println(arr.mkString("",",","")))
    val model = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(10)
      .run(unique)

    //查看每种组合出现的频次（次数）
    val freqItemsets = model.freqItemsets.map { itemset =>
      itemset.items.mkString("[", ",", "]") + ", " + itemset.freq
    }
    freqItemsets.take(3).foreach(println)
    freqItemsets.repartition(1).saveAsTextFile("C:/Users/cjp/Desktop/0330/new_teams_userAndTeams.csv/FPGrowth_freqItemsets")

    println("=======================================================")
    //查看每种组合=>衍生情况发生置信度
    val generateAssociationRules = model.generateAssociationRules(minConfidence).map { rule => {
      rule.antecedent.mkString("[", ",", "]") + " => " + rule.consequent.mkString("[", ",", "]") + ", " + rule.confidence
    }
    }
    generateAssociationRules.take(3).foreach(println)
    generateAssociationRules.repartition(1).saveAsTextFile("C:/Users/cjp/Desktop/0330/new_teams_userAndTeams.csv/FPGrowth_generateAssociationRules")
    sc.stop()
  }
}
