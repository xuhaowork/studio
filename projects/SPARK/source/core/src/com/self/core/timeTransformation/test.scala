package com.self.core.timeTransformation

import java.text.SimpleDateFormat

import com.self.core.baseApp.myAPP


/**
  * editor: xuhao
  * date: 2018.10.30 09:00:00
  */

object test extends myAPP {
  def createData(): Unit = {
    val times = Array(
      "Mon Jan 08 18:22:03 CST 2018",
      "2018-01-08 18:22:03",
      "2018/01/08 18:22:03",
      "2018-01-08星期一 18:22:03",
      "2018-01-08Mon 18:22:03",
      "2018-01-08T10:22:03Z",
      "2018-01-08Mon 18:22:03",
      "2018-01-08 Mon 18:22:03",
      "2018-01-08T11:22:03+01:00",
      "2018-01-08T11:22:03.0+0100",
      "2018-01-08 11:22:03+01:00",
      "Mon, 08 Jan 2018 10:22:03 GMT",
      "Mon 08 Jan 2018 18:22:03 GMT+0800"
    )

    val times2 = Array(
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018",
      "Mon Jan 08 18:22:03 CST 2018"
    )

    val u = sqlc.createDataFrame(times2.map(Tuple1.apply)).toDF("time")
    val name = "数据1_us0qAH3C"
    outputrdd.put(name, u)
  }


//  def test(): Unit = {
//    createData()
//
//    import java.sql.Timestamp
//    import java.text.SimpleDateFormat
//
//    import com.google.gson.JsonParser
//    import com.zzjz.deepinsight.core.timeTransformation.models.{StringTimeFormatMatcher, TimeFormatParser, UDFCreator}
//    import com.zzjz.deepinsight.core.utils.pretreatmentUtils
//    import org.apache.spark.sql.expressions.UserDefinedFunction
//    import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
//
//    val jsonParam = """{"RERUNNING":{"nodeName":"时间格式转换1","preNodes":[{"checked":true,"id":"数据1_us0qAH3C"}],"rerun":"false"},"inputTableName":"数据1_us0qAH3C","outputName":"outputCol","outputTimeFieldFormat":{"unit":"millisecond","value":"utc"},"timeFieldFormat":{"detectFormat":{"takeNum":"10","value":"take"},"value":"selfAdapt"},"timeFieldName":[{"__initFieldSchema__":true,"datatype":"string","index":0,"name":"time"}]}"""
//
//    val z1 = z
//    val parser = new JsonParser()
//    val pJsonParser = parser.parse(jsonParam).getAsJsonObject
//
//    /** 1)获取DataFrame */
//    val tableName = pJsonParser.get("inputTableName").getAsString
//    val rawDataFrame = z1.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
//    try {
//      rawDataFrame.schema.fieldNames.length
//    } catch {
//      case _: Exception => throw new Exception(s"在获得数据'$tableName'时失败，请确保您填入的是上一个结点输出的数据")
//    }
//
//    /** 2)字段处理 */
//    val timeFieldName = pJsonParser.get("timeFieldName").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
//    pretreatmentUtils.columnExists(timeFieldName, rawDataFrame, true)
//    val timeColType = rawDataFrame.schema(timeFieldName).dataType
//    import org.apache.spark.sql.columnUtils.DataTypeImpl._
//    require(
//      timeColType in Seq("string", "timestamp", "long"),
//      s"需要'$timeFieldName'是String类型(时间字符串)，Timestamp或Long类型(UTC时间戳)"
//    )
//
//    /** 3)时间字段类型处理 */
//    // 输入时间字段
//    val timeFieldFormatObj = pJsonParser.get("timeFieldFormat").getAsJsonObject
//    // 输出时间字段
//    val outputTimeFieldFormat = pJsonParser.get("outputTimeFieldFormat").getAsJsonObject
//    val performanceInfo = outputTimeFieldFormat.get("value").getAsString match {
//      case "string" =>
//        val timeInputFormat = outputTimeFieldFormat.get("timeInputFormat").getAsJsonObject
//        ("string", timeInputFormat.get("timeFormat").getAsString)
//      case "Timestamp" =>
//        ("Timestamp", "")
//      case "utc" =>
//        ("UTC", outputTimeFieldFormat.get("unit").getAsString)
//
//    }
//
//    /** 4)时间字符串字段解析处理 --只有时间列为string类型时才成立 */
//    // utc时间戳 --只有时间列为UTC时间戳是才用到，默认为millisecond
//    val parserFunction: UserDefinedFunction = timeFieldFormatObj.get("value").getAsString match {
//      case "selfAdapt" =>
//        val detectFormatObj = timeFieldFormatObj.get("detectFormat").getAsJsonObject
//
//        rawDataFrame.schema(timeFieldName).dataType match {
//          case StringType =>
//            val stringParser = detectFormatObj.get("value").getAsString match {
//              case "foreach" =>
//                (time: String) =>
//                  new StringTimeFormatMatcher().parse(time)
//
//              case "take" =>
//                // @todo: 写一个参数转为double, 数值等类型的工具类
//                val takeNum = detectFormatObj.get("takeNum").getAsString
//                val num = try {
//                  takeNum.toDouble.toInt
//                } catch {
//                  case e: Exception => throw new Exception(
//                    s"将前若干项训练获得匹配模式中的数据条数'$takeNum'转为数值类型时失败，具体信息: ${e.getMessage}")
//                }
//                val parserFormat = new TimeFormatParser().setTimeCol(timeFieldName).setMaxRecords4train(num).train(rawDataFrame)
//                (time: String) =>
//                  util.Try(new Timestamp(parserFormat.parse(time).getTime)).getOrElse(null)
//            }
//            UDFCreator.createUDF(StringType, None, Some(stringParser), performanceInfo)
//
//          case LongType =>
//            UDFCreator.createUDF(LongType, Some("millisecond"), None, performanceInfo)
//
//          case TimestampType =>
//            UDFCreator.createUDF(TimestampType, None, None, performanceInfo)
//        }
//
//      case "byHand" =>
//        val fieldFormat = timeFieldFormatObj.get("fieldFormat").getAsJsonObject
//        fieldFormat.get("value").getAsString match {
//          case "Timestamp" =>
//            require(
//              timeColType == TimestampType,
//              s"您输入的时间列类型为Timestamp, 而实际类型为: ${timeColType.simpleString}"
//            )
//            UDFCreator.createUDF(TimestampType, None, None, performanceInfo)
//
//          case "UTC" =>
//            require(
//              timeColType == LongType,
//              s"您输入的时间列类型为UTC时间, 需要是长整型, 而实际类型为: ${timeColType.simpleString}"
//            )
//            val utcUnit = fieldFormat.get("unit").getAsString
//            UDFCreator.createUDF(LongType, Some(utcUnit), None, performanceInfo)
//
//          case "string" =>
//            require(
//              timeColType == StringType,
//              s"您输入的时间列类型为UTC时间, 需要是长整型, 而实际类型为: ${timeColType.simpleString}"
//            )
//            val timeFormatInputFormat = fieldFormat.get("timeFormatInputFormat").getAsJsonObject
//            val timeFormat = timeFormatInputFormat.get("timeFormat").getAsString
//            val simpleDateFormat = new SimpleDateFormat(timeFormat)
//            val stringParser = (time: String) =>
//              util.Try(new Timestamp(simpleDateFormat.parse(time).getTime)).getOrElse(null)
//
//            UDFCreator.createUDF(StringType, None, Some(stringParser), performanceInfo)
//        }
//
//    }
//
//    val outputColName = pJsonParser.get("outputName").getAsString
//    import org.apache.spark.sql.functions.col
//    rawDataFrame.withColumn(outputColName, parserFunction(col(timeFieldName))).show()
//
//  }

//  def zzjzScripts(): Unit = {
//    import java.sql.Timestamp
//    import java.text.SimpleDateFormat
//
//    import com.google.gson.JsonParser
//    import com.zzjz.deepinsight.core.timeTransformation.models.{StringTimeFormatMatcher, TimeFormatParser, UDFCreator}
//    import com.zzjz.deepinsight.core.utils.pretreatmentUtils
//    import org.apache.spark.sql.columnUtils.DataTypeImpl._
//    import org.apache.spark.sql.expressions.UserDefinedFunction
//    import org.apache.spark.sql.functions.col
//    import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
//
//    val jsonParam = "<#zzjzParam#>"
//
//    val z1 = z
//    val parser = new JsonParser()
//    val pJsonParser = parser.parse(jsonParam).getAsJsonObject
//    val rddTableName = "<#zzjzRddName#>"
//
//    /** 1)获取DataFrame */
//    val tableName = pJsonParser.get("inputTableName").getAsString
//    val rawDataFrame = z1.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
//    try {
//      rawDataFrame.schema.fieldNames.length
//    } catch {
//      case _: Exception => throw new Exception(s"在获得数据'$tableName'时失败，请确保您填入的是上一个结点输出的数据")
//    }
//
//    /** 2)字段处理 */
//    val timeFieldName = pJsonParser.get("timeFieldName").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
//    pretreatmentUtils.columnExists(timeFieldName, rawDataFrame, true)
//    val timeColType = rawDataFrame.schema(timeFieldName).dataType
//    require(
//      timeColType in Seq("string", "timestamp", "long"),
//      s"需要'$timeFieldName'是String类型(时间字符串)，Timestamp或Long类型(UTC时间戳)"
//    )
//
//    /** 3)时间字段类型处理 */
//    // 输入时间字段
//    val timeFieldFormatObj = pJsonParser.get("timeFieldFormat").getAsJsonObject
//    // 输出时间字段
//    val outputTimeFieldFormat = pJsonParser.get("outputTimeFieldFormat").getAsJsonObject
//    val performanceInfo = outputTimeFieldFormat.get("value").getAsString match {
//      case "string" =>
//        val timeInputFormat = outputTimeFieldFormat.get("timeInputFormat").getAsJsonObject
//        ("string", timeInputFormat.get("timeFormat").getAsString)
//      case "Timestamp" =>
//        ("Timestamp", "")
//      case "utc" =>
//        ("UTC", outputTimeFieldFormat.get("unit").getAsString)
//
//    }
//
//    /** 4)时间字符串字段解析处理 --只有时间列为string类型时才成立 */
//    // utc时间戳 --只有时间列为UTC时间戳是才用到，默认为millisecond
//    val parserFunction: UserDefinedFunction = timeFieldFormatObj.get("value").getAsString match {
//      case "selfAdapt" =>
//        val detectFormatObj = timeFieldFormatObj.get("detectFormat").getAsJsonObject
//
//        rawDataFrame.schema(timeFieldName).dataType match {
//          case StringType =>
//            val stringParser = detectFormatObj.get("value").getAsString match {
//              case "foreach" =>
//                (time: String) =>
//                  new StringTimeFormatMatcher().parse(time)
//
//              case "take" =>
//                // @todo: 写一个参数转为double, 数值等类型的工具类
//                val takeNum = detectFormatObj.get("takeNum").getAsString
//                val num = try {
//                  takeNum.toDouble.toInt
//                } catch {
//                  case e: Exception => throw new Exception(
//                    s"将前若干项训练获得匹配模式中的数据条数'$takeNum'转为数值类型时失败，具体信息: ${e.getMessage}")
//                }
//                val parserFormat = new TimeFormatParser().setTimeCol(timeFieldName).setMaxRecords4train(num).train(rawDataFrame)
//                (time: String) =>
//                  util.Try(new Timestamp(parserFormat.parse(time).getTime)).getOrElse(null)
//            }
//            UDFCreator.createUDF(StringType, None, Some(stringParser), performanceInfo)
//
//          case LongType =>
//            UDFCreator.createUDF(LongType, Some("millisecond"), None, performanceInfo)
//
//          case TimestampType =>
//            UDFCreator.createUDF(TimestampType, None, None, performanceInfo)
//        }
//
//      case "byHand" =>
//        val fieldFormat = timeFieldFormatObj.get("fieldFormat").getAsJsonObject
//        fieldFormat.get("value").getAsString match {
//          case "Timestamp" =>
//            require(
//              timeColType == TimestampType,
//              s"您输入的时间列类型为Timestamp, 而实际类型为: ${timeColType.simpleString}"
//            )
//            UDFCreator.createUDF(TimestampType, None, None, performanceInfo)
//
//          case "UTC" =>
//            require(
//              timeColType == LongType,
//              s"您输入的时间列类型为UTC时间, 需要是长整型, 而实际类型为: ${timeColType.simpleString}"
//            )
//            val utcUnit = fieldFormat.get("unit").getAsString
//            UDFCreator.createUDF(LongType, Some(utcUnit), None, performanceInfo)
//
//          case "string" =>
//            require(
//              timeColType == StringType,
//              s"您输入的时间列类型为UTC时间, 需要是长整型, 而实际类型为: ${timeColType.simpleString}"
//            )
//            val timeFormatInputFormat = fieldFormat.get("timeFormatInputFormat").getAsJsonObject
//            val timeFormat = timeFormatInputFormat.get("timeFormat").getAsString
//            val simpleDateFormat = new SimpleDateFormat(timeFormat)
//            val stringParser = (time: String) =>
//              util.Try(new Timestamp(simpleDateFormat.parse(time).getTime)).getOrElse(null)
//
//            UDFCreator.createUDF(StringType, None, Some(stringParser), performanceInfo)
//        }
//
//    }
//
//    val outputColName = pJsonParser.get("outputName").getAsString
//    val newDataFrame = rawDataFrame.withColumn(outputColName, parserFunction(col(timeFieldName)))
//
//    newDataFrame.createOrReplaceTempView(rddTableName)
//    newDataFrame.sqlContext.cacheTable(rddTableName)
//    outputrdd.put(rddTableName, newDataFrame)
//
//
//  }

  override def run(): Unit = {
//    test()

//    sc.textFile("")
//
//    val df = sqlc.createDataFrame(
//      Seq(
//        (1, 0, "A"),
//        (2, 2, "B"),
//        (1, 2, "C"),
//        (1, 2, "D"),
//        (2, 0, "E"),
//        (1, 0, "F")
//      )
//    ).toDF("col1", "col2", "col3")
//
//    import org.apache.spark.sql.functions.{array, coalesce, col, collect_list, collect_set, first, min}
//
//    df.groupBy(col("col1")).agg(min(col("col2")), first(col("col3"))).show()

    val tf = new SimpleDateFormat("dd-MM月-yy")
    println(tf.parse("13-11月-18"))



  }
}
