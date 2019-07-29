//package org.apache.spark.sql.execution.datasources.util
//
//import com.lucky.shipyard.common.job.node.{DataSourceSetting, Field}
//import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
//
//import scala.collection.JavaConversions._
//
///**
//  * @define TODO
//  * @author han.wang at 2019/1/12 15:13
//  */
//object SchemaUtil {
//
//  /**
//    * 组装数据源的元数据格式
//    * @param dataSourceSetting 数据源配置
//    * @return 元数据格式
//    */
//  def parseSchema(dataSourceSetting: DataSourceSetting): Option[StructType] = {
//    if(dataSourceSetting.getSchema == null
//      || dataSourceSetting.getSchema.getFields == null
//      || dataSourceSetting.getSchema.getFields.isEmpty) {
//      Option(null)
//    } else {
//      Option(parseSchema(dataSourceSetting.getSchema.getFields))
//    }
//
//  }
//
//  /**
//    * 将定义的字段转换为schema
//    * @param fieldSettings 用户定义的字段
//    * @return rdd schema
//    */
//  def parseSchema(fieldSettings: Iterable[Field]): StructType = {
//    var fields: Seq[StructField] = Seq[StructField]()
//    for(field <- fieldSettings) {
//      fields :+= parseStructField(field)
//    }
//    StructType(fields)
//  }
//
//  /**
//    * 转化为元数据中的字段
//    * @param field 字段定义
//    * @return 元数据中的字段
//    */
//  def parseStructField(field: Field): StructField = {
//    val fieldType = Field.FieldType.valueOf(field.getFieldType)
//    fieldType match {
//      case Field.FieldType.BINARY => StructField(field.getFieldName, DataTypes.BinaryType)
//      case Field.FieldType.BOOLEAN => StructField(field.getFieldName, DataTypes.BooleanType)
//      case Field.FieldType.DATE => StructField(field.getFieldName, DataTypes.DateType)
//      case Field.FieldType.DECIMAL => {
//        if(field.getPrecision!=null && field.getScale!=null) {
//          StructField(field.getFieldName, DataTypes.createDecimalType(field.getPrecision, field.getScale))
//        } else {
//          StructField(field.getFieldName, DataTypes.createDecimalType())
//        }
//      }
//      case Field.FieldType.DOUBLE => StructField(field.getFieldName, DataTypes.DoubleType)
//      case Field.FieldType.INTEGER => StructField(field.getFieldName, DataTypes.IntegerType)
//      case Field.FieldType.LONG => StructField(field.getFieldName, DataTypes.LongType)
//      case Field.FieldType.SHORT => StructField(field.getFieldName, DataTypes.ShortType)
//      case Field.FieldType.STRING => StructField(field.getFieldName, DataTypes.StringType)
//      case Field.FieldType.TIMESTAMP => StructField(field.getFieldName, DataTypes.TimestampType)
//      case Field.FieldType.MAP => StructField(field.getFieldName, parseSchema(field.getNestedFields))
//      case _ => throw new IllegalArgumentException(String.format("field {}'s type {} is illegal", field.getFieldName, field.getFieldType))
//    }
//  }
//
//}
