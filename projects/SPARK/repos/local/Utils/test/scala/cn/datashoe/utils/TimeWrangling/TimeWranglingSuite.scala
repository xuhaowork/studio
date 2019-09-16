package cn.datashoe.utils.TimeWrangling

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone

import org.scalatest.FunSuite

/**
  * TimeWrangling的Suite测试类
  */
class TimeWranglingSuite() extends FunSuite {
  test("测试长整型时间的格式化") {
    val time = new TimeWithLongParser()
      .setEPOCH(TimeWithLongParser.EPOCH_FORMAT.FILE_TIME_UTC)
      .setPrecision(100)
      .setTimeUnit(TimeWithLongParser.TIME_UNIT.NANOSECOND)
      .parser(129757574870723241L)

    println(time)

    // 对比:
    // 129757574870723241L
    // 129757574880723241L
    val time2 = new TimeWithLongParser()
      .setEPOCH(TimeWithLongParser.EPOCH_FORMAT.FILE_TIME_UTC)
      .setPrecision(100)
      .setTimeUnit(TimeWithLongParser.TIME_UNIT.NANOSECOND)
      .parser(129757574870733241L)

    // 检测, 不发生上溢的情况下是对的
    require(time == new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse("2012-03-09 17:04:47.072").getTime))
    println(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(time))

    require(time2 == new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse("2012-03-09 17:04:47.073").getTime))
    println(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(time2))

    intercept[IllegalArgumentException]{
      require(time2 == new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse("2012-03-09 17:04:47.074").getTime))
    }

  }

  test("系统支持`Etc/GMT`类型作为时区配置") {
    require(TimeZone.getAvailableIDs contains "Etc/GMT")
    println(TimeZone.getAvailableIDs.mkString(", "))

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(sdf.parse("1970-01-01 00:00:00"))
    sdf.setTimeZone(TimeZone.getTimeZone("Etc/GMT"))
    println(sdf.parse("1970-01-01 00:00:00"))
    sdf.setTimeZone(TimeZone.getTimeZone("Etc/GMT-12"))
    println(sdf.parse("1970-01-01 00:00:00"))
    sdf.setTimeZone(TimeZone.getTimeZone("Etc/GMT+12"))
    println(sdf.parse("1970-01-01 00:00:00"))
  }


  test("时间字符串格式化") {
    val now = new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2019-03-27 09:30:00").getTime)
    val s1 = new StringTimeFormatCreator().setTimeFormat("yyyy-MM-dd'T'HH:mm:ssXXX").create().format(now)
    val s2 = new StringTimeFormatCreator().setTimeFormat("yyyy-MM-dd'T'HH:mm:ssXXX").setZone(9).create().format(now)
    println(s1)
    println(s2)

    // 当前时区需要为东8区
    require(s1 == "2019-03-27T09:30:00+08:00")
    // 设置9需要为东9区, 东9区比东8区快1个小时
    require(s2 == "2019-03-27T10:30:00+09:00")

    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Timestamp(-110014009196L)))
  }


}
