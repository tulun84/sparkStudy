package catalyst_ext

import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/**
  * 自定义 SparkSQL catalyst解析器的优化器——Optimizer
  * （当表达式中出现 "*1" 时，忽略该行为——因为任何数据*1，都等于它本身）
  *
  * @param spark
  */
case class MultiplyOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case mul@Multiply(left, right) if right.isInstanceOf[Literal] && right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
      left
  }
}

object MultiplyOptimizationRule {
  val extBuilder = (e: SparkSessionExtensions) => e.injectOptimizerRule(ss => new MultiplyOptimizationRule(ss))
}

