package catalyst_ext

import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/**
  * @Date: 2019/6/26 14:45
  *        自定义 SparkSQL catalyst解析器的解析器——Parser
  *        (Parser SQL成Unresove Logincal Plan阶段，进行自定义校验，本例中：只要出现"select *"行为，就会抛出异常。)
  */
case class StrictParser(parser: ParserInterface) extends ParserInterface {

  /**
    * Parse a string to a [[LogicalPlan]].
    */
  override def parsePlan(sqlText: String): LogicalPlan = {
    val logicalPlan = parser.parsePlan(sqlText)
    logicalPlan.transform {
      case project@Project(projectList, _) =>
        projectList.foreach {
          project => {
            if (project.isInstanceOf[UnresolvedStar]) {
              throw new RuntimeException("You must specify your project column set, * is not allowed.")
            }
          }

        }
        project
    }
    logicalPlan
  }

  /**
    * Parse a string to an [[Expression]].
    */
  override def parseExpression(sqlText: String): Expression = parser.parseExpression(sqlText)

  /**
    * Parse a string to a [[TableIdentifier]].
    */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = parser.parseTableIdentifier(sqlText)

  /**
    * Parse a string to a [[FunctionIdentifier]].
    */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = parser.parseFunctionIdentifier(sqlText)

  /**
    * Parse a string to a [[StructType]]. The passed SQL string should be a comma separated
    * list of field definitions which will preserve the correct Hive metadata.
    */
  override def parseTableSchema(sqlText: String): StructType = parser.parseTableSchema(sqlText)

  /**
    * Parse a string to a [[DataType]].
    */
  override def parseDataType(sqlText: String): DataType = parser.parseDataType(sqlText)
}

object StrictParser {
  /*========创建扩展点函数  开始 =========*/
  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  type ExtensionsBuilder = SparkSessionExtensions => Unit
  val parserBuilder: ParserBuilder = (_, parser) => new StrictParser(parser)
  val extBuilder: ExtensionsBuilder = e => e.injectParser(parserBuilder)
  /*========创建扩展点函数  结束 =========*/
}
