package org.apache.spark.mllib.feature

import java.sql.Timestamp

import com.google.gson.JsonObject
import com.self.core.generalTimeBinner.models._
import org.apache.spark.SparkException
import org.apache.spark.sql.NullableFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.expressions.UserDefinedFunction


object BinningUtils extends Serializable {
  val unitMatch: Map[String, Long] = Map(
    "year" -> 3600L * 24L * 365L * 1000L, // 约数
    "month" -> 3600L * 24L * 30L * 1000L, // 约数
    "week" -> 3600L * 24L * 7L * 1000L,
    "day" -> 3600L * 24L * 1000L,
    "hour" -> 3600L * 1000L,
    "minute" -> 60L * 1000L,
    "second" -> 1000L,
    "millisecond" -> 1L
  )

  val adaptiveTimeFormat4Unit: Map[String, String] = Map(
    "year" -> "yyyy年MM月dd日 HH:mm:ss", // 约数
    "month" -> "MM月dd日 HH:mm:ss", // 约数
    "week" -> "MM月dd日 HH:mm:ss",
    "day" -> "dd日 HH:mm:ss",
    "hour" -> "HH:mm:ss",
    "minute" -> "HH:mm:ss",
    "second" -> "HH:mm:ss",
    "millisecond" -> "HH:mm:ss.SSS"
  )


  def adaptTimeFormat(window: Long): String = {
    var timeFormat = ""
    val unitCollection = Iterator("year", "month", "week", "day",
      "hour", "minute", "second", "millisecond")
    var findAGirl = false
    var grade = 0
    while (unitCollection.hasNext && !findAGirl) {
      val nextUnit = unitCollection.next()
      if (window >= unitMatch(nextUnit)) {
        timeFormat = adaptiveTimeFormat4Unit(nextUnit)
        findAGirl = true
      }
      grade += 1
    }
    timeFormat
  }


  private[spark] def reConstruct(arr: Array[(TimeMeta, Int, Int, Int)]): BinningTime = {
    val values: Map[(Int, Int), (TimeMeta, Int)] = arr.map {
      case (element, dpth, pstn, parentPosition) =>
        ((dpth, pstn), (element, parentPosition))
    }.toMap
    val deep = arr.map(_._2).max
    BinningUtils.constructBinningTimeFromArray(values, deep, -1)
  }


  /** 搞定改成的两个节点形成一个树，直至根节点 */
  private[spark]
  def constructBinningTimeFromArray(
                                     values: Map[(Int, Int), (TimeMeta, Int)],
                                     deep: Int,
                                     position: Int
                                   ): BinningTime = {
    if (deep == 0) { // 根节点时终止
      new BinningTime(values(deep, position)._1, None, None, true, deep, position, values(deep, position)._2)
    } else {
      val tree = constructBinningTimeFromArray(
        values, deep - 1, values(deep, position)._2)
      val bt = tree.getTipNode(values(deep, position)._2 == -1) // 获得整个树的最后分支

      if (position < 0) { // 左子树, 此时dpth必然大于0
        bt.setLeftChild(
          new BinningTime(values(deep, position)._1,
            None, None, true, deep, position, values(deep, position)._2))
        bt.setRightChild(
          new BinningTime(values(deep, position * -1)._1,
            None, None, true, deep, position, values(deep, position * -1)._2))
      } else {
        bt.setRightChild(
          new BinningTime(values(deep, position)._1,
            None, None, true, deep, position, values(deep, position)._2))
        bt.setLeftChild(
          new BinningTime(values(deep, position * -1)._1,
            None, None, true, deep, position, values(deep, position * -1)._2))
      }
      tree
    }
  }


  private[spark] def serializeForTimeMeta(obj: Any): InternalRow = obj match {
    case absMT: AbsTimeMeta =>
      val row = new GenericMutableRow(4)
      row.setByte(0, 0)
      row.setLong(1, absMT.value)
      if (absMT.rightBound.isDefined)
        row.setLong(2, absMT.rightBound.get)
      else
        row.setNullAt(2)
      absMT.timeFormat.map(char => char.toInt).toArray
      row.update(3, new GenericArrayData(absMT.timeFormat.map(char => char.toInt).toArray))
      row
    case rlMT: RelativeMeta =>
      val arr = rlMT.timeFormat.map(char => char)
      val row = new GenericMutableRow(3 + arr.length)
      row.setByte(0, 1)
      row.setLong(1, rlMT.value)
      if (rlMT.rightBound.isDefined)
        row.setLong(2, rlMT.rightBound.get)
      else
        row.setNullAt(2)
      row.update(3, new GenericArrayData(rlMT.timeFormat.map(char => char.toInt).toArray))
      row
    case _ =>
      throw new Exception("您输入类型不是TimeMeta在spark sql中没有预定义的序列化方法")
  }

  private[spark] def deserializeForTimeMeta(datum: Any): TimeMeta =
    try {
      datum match {
        case row: InternalRow =>
          val mtType = row.getByte(0)
          val value = row.getLong(1)
          val rightBound = if (row.isNullAt(2)) None else Some(row.getLong(2))
          val timeFormat = row.getArray(3).toIntArray().map(_.toChar).mkString("")
          if (mtType == 0) {
            new AbsTimeMeta(value, rightBound, timeFormat)
          } else {
            new RelativeMeta(value, rightBound, timeFormat)
          }
      }
    } catch {
      case e: Exception => throw new SparkException(s"您输入的类型为:${datum.getClass.getSimpleName}, 具体信息${e.getMessage}")
    }

}


  /**
    * 构造分箱UDF的工具类 --该类不必序列化
    * ----
    *
    * @define readFromJson 提供一个从JsonObject到[[com.self.core.generalTimeBinner.models.PerformanceInfo]]的转换，转为能序列化的类型，方便在UDF中调用
    * @define constructUDF 提供一个构建UDF的函数 需要解析器，分箱信息，展示信息三种信息
    *
    */
  object BinningUDFUtil {
    /** 将参数从JsonObject中解析出来存到能够序列化的PerformanceInfo容器中 */
    def readFromJson(performanceTypeObj: JsonObject): PerformanceInfo =
      performanceTypeObj.get("value").getAsString match {
        case "binningTime" =>
          new BinningTimePfm
        case "binningResult" =>
          val byInterval = performanceTypeObj.get("byInterval").getAsString

          val resultTypeObj = performanceTypeObj.get("resultType").getAsJsonObject
          val resultType = resultTypeObj.get("value").getAsString match {
            case "string" =>
              val timeFormatTypeObj = resultTypeObj.get("timeFormatType").getAsJsonObject
              timeFormatTypeObj.get("value").getAsString match {
                case "byHand" =>
                  val handScript = timeFormatTypeObj.get("handScript").getAsString
                  require(handScript.length > 0, "您输入的时间字符串展示格式为空")
                  new StringResultType("byHand", Some(handScript), None)

                case "select" =>
                  val select = timeFormatTypeObj.get("select").getAsString
                  new StringResultType("select", None, Some(select))

                case "selfAdapt" =>
                  new StringResultType("selfAdapt", None, None)
              }

            case "long" =>
              new LongResultType(resultTypeObj.get("unit").getAsString)

            case "timestamp" =>
              new TimestampResultType
          }
          new BinningResultPfm(byInterval, resultType)
      }


    def constructUDF(
                      timeParser: TimeParser,
                      binningInfo: TimeBinnerInfo,
                      performanceInfo: PerformanceInfo
                    ): UserDefinedFunction = {
      performanceInfo match {
        case _: BinningTimePfm =>
          NullableFunctions.udf(
            (any: Any) => {
              /** 解析为BinningTime */
              val parserTime: BinningTime = timeParser.parse(any)
              parserTime.binning(binningInfo)
            })

        case bs: BinningResultPfm =>
          // "interval", "left", "right"
          val byInterval = bs.byInterval
          // "string" // "long" // "timestamp"
          val resultType = bs.resultType
          resultType match {
            // string是udf始终是BinningTime => string
            case st: StringResultType =>
              val timeFormatType = st.timeFormatType
              val timeFormat = timeFormatType match {
                case "byHand" =>
                  Some(st.handScript.get)
                case "select" =>
                  Some(st.select.get)
                case "selfAdapt" =>
                  None
              }

              NullableFunctions.udf(
                (any: Any) => {
                  /** 解析为BinningTime */
                  val parserTime: BinningTime = timeParser.parse(any)
                  parserTime.binning(binningInfo)
                  parserTime.getBinningResultToString(timeFormat, byInterval)
                })

            case lt: LongResultType => // long以区间展示是string类型，其他为long类型
              val unit = lt.unit
              if (byInterval == "interval") {
                NullableFunctions.udf(
                  (any: Any) => {
                    /** 解析为BinningTime */
                    val parserTime: BinningTime = timeParser.parse(any)
                    parserTime.binning(binningInfo)
                    val left =
                      if (unit == "second")
                        parserTime.getBinningBySide("left").getMillis / 1000
                      else
                        parserTime.getBinningBySide("left").getMillis
                    val right =
                      if (unit == "second")
                        parserTime.getBinningBySide("left").getMillis / 1000
                      else
                        parserTime.getBinningBySide("left").getMillis
                    "[" + left + ", " + right + "]"
                  })
              } else {
                NullableFunctions.udf(
                  (any: Any) => {
                    /** 解析为BinningTime */
                    val parserTime: BinningTime = timeParser.parse(any)
                    parserTime.binning(binningInfo)

                    if (unit == "second")
                      parserTime.getBinningBySide(byInterval).getMillis / 1000
                    else
                      parserTime.getBinningBySide(byInterval).getMillis
                  })
              }

            case _: TimestampResultType =>
              NullableFunctions.udf(
                (any: Any) => {
                  /** 解析为BinningTime */
                  val parserTime: BinningTime = timeParser.parse(any)
                  parserTime.binning(binningInfo)
                  val millis = parserTime.getBinningBySide(byInterval).getMillis
                  new Timestamp(millis)
                })
          }

      }

    }

  }


  /**
    *
    * @param timeFormat 时间格式
    * @param grade      分箱对应单位的级别
    *                   当分箱长度大于等于1年时，级别为0，展示时间格式"yyyy年MM月dd日 HH:mm:ss"
    *                   当分箱长度小于1年大于等于1月时，级别为1，展示时间格式为"MM月dd日 HH:mm:ss"
    *                   当分箱长度小于1月大于等于1星期时，级别为2，展示时间格式为"MM月dd日 HH:mm:ss"
    *                   当分箱长度小于1星期大于等于1天时，级别为3，展示时间格式为"dd日 HH:mm:ss"
    *                   当分箱长度小于1天大于等于1小时时，级别为4，展示时间格式为"HH:mm:ss",
    *                   当分箱长度小于1小时大于等于1分钟时，级别为5，展示时间格式为"HH:mm:ss",
    *                   当分箱长度小于1分钟大于等于1秒钟时，级别为6，展示时间格式为"HH:mm:ss",
    *                   当分箱长度小于1秒钟大于等于1毫秒时，级别为7，展示时间格式为"HH:mm:ss.SSS"
    */
  private[spark] case class AdaptiveTimeFormat(timeFormat: String, grade: Int)

