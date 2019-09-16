package com.self.core.timeTransformation.models

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.JsonObject
import org.apache.spark.sql.{UDFDealWithNull, UserDefinedFunction}


/** 创建一个时间转换的udf */
object UDFCreator {
  def udfCreate(
                 inputTimeFieldObj: JsonObject,
                 pJsonParser: JsonObject,
                 fillByTime: Boolean,
                 fillTime: Timestamp
               ): UserDefinedFunction = {
    // 输入的时间字段信息
    val inputFieldType = inputTimeFieldObj.get("value").getAsString
    // 输出的时间字段信息
    val outputTimeFieldFormat = pJsonParser.get("outputTimeFieldFormat").getAsJsonObject
    val performanceInfo = outputTimeFieldFormat.get("value").getAsString match {
      case "TimeString" =>
        val timeInputFormat = outputTimeFieldFormat.get("timeInputFormat").getAsJsonObject
        ("TimeString", timeInputFormat.get("timeFormat").getAsString)
      case "Timestamp" =>
        ("Timestamp", "")
      case "UTC" =>
        ("UTC", outputTimeFieldFormat.get("unit").getAsString)
    }

    inputFieldType match {
      case "TimeString" =>
        val timeFormatArray = inputTimeFieldObj.get("timeFormatArray").getAsJsonArray
        val inputTimeFormats = Seq.range(0, timeFormatArray.size()).map {
          i =>
            val timeFormatObj = timeFormatArray.get(i).getAsJsonObject
            timeFormatObj.get("timeFormatObj").getAsJsonObject.get("timeFormat").getAsString
        }
        val stringTFMatcher = if (inputTimeFieldObj.get("byRepository").getAsString == "true") {
          new StringTimeFormatMatcher().appendTimeFormat(inputTimeFormats)
        }
        else
          new StringTimeFormatMatcher().updateTimeFormat(inputTimeFormats)

        performanceInfo match {
          case ("TimeString", castTimeFormat) =>
            val castTf = try {
              new SimpleDateFormat(castTimeFormat)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的输出时间字符串格式'$castTimeFormat'无法转为正确的时间字符串样式" + s"具体信息: ${e.getMessage}"
              )
            }
            UDFDealWithNull.udfDealWithOption {
              (time: String) =>
                val res = util.Try(castTf.format(new Date(stringTFMatcher.parse(time).getTime))).toOption
                if (fillByTime && res.isEmpty) Some(castTf.format(new Date(fillTime.getTime))) else res
            }

          case ("UTC", unit) =>
            if (unit == "millisecond") {
              UDFDealWithNull.udfDealWithOption {
                (time: String) =>
                  val res = util.Try(new Date(stringTFMatcher.parse(time).getTime)).toOption
                  if (fillByTime && res.isEmpty) Some(fillTime.getTime) else res
              }
            } else {
              UDFDealWithNull.udfDealWithOption {
                (time: String) =>
                  val res = util.Try(new Date(stringTFMatcher.parse(time).getTime / 1000L)).toOption
                  if (fillByTime && res.isEmpty) Some(fillTime.getTime / 1000L) else res
              }
            }

          case ("Timestamp", "") =>
            UDFDealWithNull.udfDealWithOption {
              (time: String) =>
                val res = util.Try(new Timestamp(stringTFMatcher.parse(time).getTime)).toOption
                if (fillByTime && res.isEmpty) Some(fillTime) else res
            }
        }

      case "UTC" =>
        val inputUnit = inputTimeFieldObj.get("unit").getAsString
        performanceInfo match {
          case ("TimeString", castTimeFormat) =>
            val castTf = try {
              new SimpleDateFormat(castTimeFormat)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的输出时间字符串格式'$castTimeFormat'无法转为正确的时间字符串样式" + s"具体信息: ${e.getMessage}"
              )
            }
            UDFDealWithNull.udfDealWithOption {
              (time: Long) =>
                val res = util.Try(
                  castTf.format(new Date(if (inputUnit == "millisecond") time else time * 1000L))
                ).toOption
                if (fillByTime && res.isEmpty) Some(castTf.format(new Date(fillTime.getTime))) else res
            }

          case ("UTC", unit) =>
            if (unit == "millisecond") {
              UDFDealWithNull.udfDealWithOption {
                (time: Long) =>
                  val res = util.Try(
                    new Date(if (inputUnit == "millisecond") time else time * 1000L).getTime
                  ).toOption
                  if (fillByTime && res.isEmpty) Some(fillTime.getTime) else res
              }
            } else {
              UDFDealWithNull.udfDealWithOption {
                (time: Long) =>
                  val res = util.Try(
                    if (inputUnit == "millisecond") time / 1000L else time
                  ).toOption
                  if (fillByTime && res.isEmpty) Some(fillTime.getTime / 1000L) else res
              }
            }

          case ("Timestamp", "") =>
            UDFDealWithNull.udfDealWithOption {
              (time: Long) =>
                val res = util.Try(
                  new Timestamp(if (inputUnit == "millisecond") time else time * 1000L)
                ).toOption
                if (fillByTime && res.isEmpty) Some(fillTime) else res
            }
        }


      case "Timestamp" =>
        performanceInfo match {
          case ("TimeString", castTimeFormat) =>
            val castTf = try {
              new SimpleDateFormat(castTimeFormat)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的输出时间字符串格式'$castTimeFormat'无法转为正确的时间字符串样式" + s"具体信息: ${e.getMessage}"
              )
            }
            UDFDealWithNull.udfDealWithOption {
              (time: Timestamp) =>
                val res = util.Try(castTf.format(new Date(time.getTime))).toOption
                if (fillByTime && res.isEmpty) Some(castTf.format(new Date(fillTime.getTime))) else res
            }

          case ("UTC", unit) =>
            if (unit == "millisecond") {
              UDFDealWithNull.udfDealWithOption {
                (time: Timestamp) =>
                  val res = util.Try(time.getTime).toOption
                  if (fillByTime && res.isEmpty) Some(fillTime.getTime) else res
              }
            } else {
              UDFDealWithNull.udfDealWithOption {
                (time: Timestamp) =>
                  val res = util.Try(time.getTime / 1000L).toOption
                  if (fillByTime && res.isEmpty) Some(fillTime.getTime / 1000L) else res
              }
            }

          case ("Timestamp", "") =>
            UDFDealWithNull.udfDealWithOption {
              (time: Timestamp) =>
                val res = util.Try(time).toOption
                if (fillByTime && res.isEmpty) Some(fillTime) else res
            }
        }

    }

  }


}
