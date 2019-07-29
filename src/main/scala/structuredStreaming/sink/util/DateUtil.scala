package structuredStreaming.sink.util

import java.util.Date

import org.apache.spark.internal.Logging

import scala.util.matching.Regex

/**
  * 日期工具
  */
object DateUtil extends Logging {
  /**
    * 日期格式
    */
  val DATE_FORMAT = "yyyy-MM-dd"

  /**
    * 时间戳格式
    */
  val TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

  /**
    * 带毫秒时间戳 毫秒位前为.
    */
  val MILLS_FORMAT_FIRST = "yyyy-MM-dd HH:mm:ss.SSS"

  /**
    * 带毫秒时间戳 毫秒位前为:
    */
  val MILLS_FORMAT_SECOND = "yyyy-MM-dd HH:mm:ss:SSS"


  val COMPLEX_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy"

  /**
    * 精确到毫秒的时间戳
    */
  val tsRegex = new Regex("""^\d{13}$""")

  /**
    * 精确到秒的时间戳
    */
  val sRegex = new Regex("""^\d{10}$""")

  /**
    * 字符串转日期
    *
    * @param dateString 日期字符串
    * @param format     时间格式
    * @return 日期对象
    */
  def parse(dateString: String, format: String): Date = {
    TimeUtils.dateStr2Date(dateString, format)
  }
//
//  /**
//    * 转为Date类型
//    *
//    * @param value 字段值
//    * @param field 字段格式
//    * @return 日期
//    */
//  def objectToDate(value: Any, field: StructField): Date = {
//    if (value.isInstanceOf[Date]) {
//      value.asInstanceOf[Date]
//    }
//    else if (value.isInstanceOf[java.sql.Date]) {
//      new Date(value.asInstanceOf[java.sql.Date].getTime)
//    } else {
//      val dateStr = value.toString()
//      if (dateStr.length == DateUtils.DATE_FORMAT.length) {
//        DateUtils.parse(dateStr, DateUtils.DATE_FORMAT)
//      } else if (dateStr.length == DateUtils.TIMESTAMP_FORMAT.length) {
//        DateUtils.parse(dateStr, DateUtils.TIMESTAMP_FORMAT)
//      } else if ((dateStr.length == DateUtils.MILLS_FORMAT_FIRST.length)
//        && dateStr.substring(dateStr.length - 4, dateStr.length - 3).equals(".")) {
//        DateUtils.parse(dateStr, DateUtils.MILLS_FORMAT_FIRST)
//      } else if ((dateStr.length == DateUtils.MILLS_FORMAT_SECOND.length)
//        && dateStr.substring(dateStr.length - 4, dateStr.length - 3).equals(":")) {
//        DateUtils.parse(dateStr, DateUtils.MILLS_FORMAT_SECOND)
//      }
//      else if ((dateStr.length == DateUtils.COMPLEX_DATE_FORMAT.length)) {
//        DateUtils.parse(dateStr, DateUtils.COMPLEX_DATE_FORMAT)
//      }
//      else {
//        logError("field " + field.name + " with wrong date format: " + dateStr)
//        null
//      }
//    }
//  }

// /**
//   * 转为时间戳
//   *
//   * @param value 字段值
//   * @param field 字段格式
//   * @return 时间戳
//   */
//  def objectToTimestamp(value: Any, field: StructField): Timestamp = {
//    if (value.isInstanceOf[Date]) {
//      new Timestamp(value.asInstanceOf[Date].getTime)
//    }
//    else if (value.isInstanceOf[java.sql.Date]) {
//      new Timestamp(value.asInstanceOf[java.sql.Date].getTime)
//    }
//    else {
//      val dateStr = value.toString()
//      logDebug("start parse date str " + dateStr + " to " + field.name)
//      var timestamp: Timestamp = null
//      var date: Date = null
//      if (dateStr.length == DateUtils.DATE_FORMAT.length) {
//        date = DateUtils.parse(dateStr, DateUtils.DATE_FORMAT)
//      } else if (dateStr.length == DateUtils.TIMESTAMP_FORMAT.length) {
//        date = DateUtils.parse(dateStr, DateUtils.TIMESTAMP_FORMAT)
//      } else if ((dateStr.length == DateUtils.MILLS_FORMAT_FIRST.length)
//        && dateStr.substring(dateStr.length - 4, dateStr.length - 3).equals(".")) {
//        date = DateUtils.parse(dateStr, DateUtils.MILLS_FORMAT_FIRST)
//      } else if ((dateStr.length == DateUtils.MILLS_FORMAT_SECOND.length)
//        && dateStr.substring(dateStr.length - 4, dateStr.length - 3).equals(":")) {
//        date = DateUtils.parse(dateStr, DateUtils.MILLS_FORMAT_SECOND)
//      }
//      else if ((dateStr.length == DateUtils.COMPLEX_DATE_FORMAT.length)) {
//        date = DateUtils.parse(dateStr, DateUtils.COMPLEX_DATE_FORMAT)
//      } else {
//        if (tsRegex.findFirstIn(dateStr).isDefined) {
//          timestamp = new Timestamp(java.lang.Long.parseLong(tsRegex.findFirstIn(dateStr).get))
//        } else if (sRegex.findFirstIn(dateStr).isDefined) {
//          timestamp = new Timestamp(java.lang.Long.parseLong(tsRegex.findFirstIn(dateStr).get) * 1000)
//        } else {
//          val msg = "field " + field.name + " with wrong date format: " + dateStr
//          logError(msg)
//          throw new RuntimeException(msg)
//        }
//      }
//      if (date != null) {
//        timestamp = new Timestamp(date.getTime)
//      }
//      timestamp
//    }
//  }

}
