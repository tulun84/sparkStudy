package com.tulun.les34

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  *
  */
object DateUtils {

  def getCurrentTime(): String =
  {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val c = Calendar.getInstance()

    sdf.format(c.getTime)

  }

  def main(args: Array[String]): Unit = {
    println("2016-09-04 15:19:09".substring(0,10))
  }
}
