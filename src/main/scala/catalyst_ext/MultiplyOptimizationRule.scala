package catalyst_ext

import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

class MultiplyOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case mul@Multiply(left, right) if right.isInstanceOf[Literal] && right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
      left
  }
}

object MultiplyOptimizationRule {
  val extBuilder = (e: SparkSessionExtensions) => e.injectOptimizerRule(spark => new MultiplyOptimizationRule(spark))
}

