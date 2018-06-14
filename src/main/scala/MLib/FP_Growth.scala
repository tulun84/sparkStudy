package MLib

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 3、支持度（support）
  * 支持度是指在所有项集中{X, Y}出现的可能性，即项集中同时含有X 和Y 的概率：
  * 设定最小阈值为5%，由亍{尿布，啤酒}的支持度为800/10000=8%，满足最小阈值要求，成为频繁项集，保留规则；而{尿布，面包}的支持度为100/10000=1%，则被剔除。
  * 4、置信度（confidence）
  * 置信度表示在先决条件X 发生的条件下，关联结果Y 发生的概率：这是生成强关联规则的第二个门槛，衡量了所考察的关联规则在“质”上的可靠性。相似地，我们需要对置信度设定最小阈值（mincon）来实现进一步筛选。
  * 当设定置信度的最小阈值为70%时，例如{尿布，啤酒}中，购买尿布时会购买啤酒的置信度为800/1000=80%，保留规则；而购买啤酒时会购买尿布的置信度为800/2000=40%，则被剔除。
  * 5. 提升度（lift）
  * 提升度表示在含有X 的条件下同时含有Y 的可能性与没有X 这个条件下项集中含有Y 的可能性之比：公式为置信度(artichok=>cracker)/支持度(cracker)。该指标与置信度同样衡量规则的可靠性，可以看作是置信度的一种互补指标。
  */
object FP_Growth {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FP_Growth").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("src/main/testFiles/sample_fpgrowth.txt")

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))
    /**
      * 假如支持度：3%，置信度：40%
      * 支持度3%：意味着3%顾客 同时 购买牛奶和面包（同时发生的概率）
      * 置信度40%：意味着购买牛奶的顾客40%也购买面包（A发生=>B发生的概率）
      */

    val minSupport = 0.2 //满足支持度阈值0.2的组合作为“频繁项集”
    //支持度
    val minConfidence = 0.2 //置信度

    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

    //查看每种组合出现的频次（次数）
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    println("=======================================================")
    //查看每种组合=>衍生情况发生置信度
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent.mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }

}
