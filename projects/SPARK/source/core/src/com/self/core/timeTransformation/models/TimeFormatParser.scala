package com.self.core.timeTransformation.models

import java.text.SimpleDateFormat

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

/**
  * 时间字符串模式匹配器
  */
class TimeFormatParser() extends Serializable {
  var timeCol: String = "timeCol"
  def setTimeCol(timeCol: String): this.type = {
    this.timeCol = timeCol
    this
  }

  var maxRecords4train: Int = 10

  val stringTimeFormatMatcher = new StringTimeFormatMatcher()

  def setMaxRecords4train(maxRecords4train: Int): this.type = {
    this.maxRecords4train = maxRecords4train
    this
  }

  def appendTimeFormat(stringTimeFormat: String, charSet: String): this.type = {
    this.stringTimeFormatMatcher.appendTimeFormat(stringTimeFormat, charSet)
    this
  }

  def train(dataSet: DataFrame): SimpleDateFormat = {
    require(dataSet.schema.fieldNames contains timeCol, s"'$timeCol'列名未能成功在表中找到，" +
      s"请检查列名是否在表中或列名是否包含().,;等转义字符")
    require(dataSet.schema(timeCol).dataType == StringType, s"需要'$timeCol'为sting类型")
    val timeFormats = dataSet.select(col(timeCol)).na.drop().take(maxRecords4train).map {
      row =>
        val timeString = row.getAs[String](0)
        stringTimeFormatMatcher.parseFormat(timeString)._2
    }.filter(_ != null).distinct
    if(timeFormats.length <= 0)
      throw new Exception(s"现有的时间字符串模式集中未能找到合适的匹配模式，您可以选择手动设置时间样式或者" +
        s"添加自动匹配模式集。")
    if(timeFormats.length > 1)
      throw new Exception(s"您的时间列'$timeCol'似乎同时包含多种时间模式, 请检查时间字符串是否是同一模式，" +
        s"如果不是建议您选择‘分别对每条记录进行模式匹配’，不过此时性能可能略低。")
    timeFormats.head
  }

}
