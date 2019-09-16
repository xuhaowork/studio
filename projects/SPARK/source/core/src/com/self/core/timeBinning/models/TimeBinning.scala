package com.self.core.timeBinning.models

import java.sql.Timestamp

import com.self.core.utils.TimeColInfo
import org.apache.spark.sql.NullableFunctions
import org.apache.spark.sql.functions.{col, unix_timestamp}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.joda.time.DateTime
import scala.math

/**
  *
  * @param timeColInfo
  * @param binningInfo
  * 所有数据转换同一精确到秒
  */
class TimeBinning(val timeColInfo: TimeColInfo,
                  val binningInfo: BinningInfo,
                  val presentInfo: PresentInfo) extends Serializable {
  var newTimeCol: TimeColInfo = {
    new TimeColInfo
  }.apply("timeCol", "long", Some("second"))
  var binningTimeCol = {
    new TimeColInfo()
  }.apply("newCol", "long", Some("second"))
  var binningTimeColBackUp: Option[TimeColInfo] = None // 用于备用的输出分箱列名，用于二级分箱


  /**
    * 时间列转换
    *
    * @param data 输入的数据
    * @return 转换后的数据，增加了一列以秒为单位的UTC时间
    */
  def transform(data: DataFrame): DataFrame = {
    timeColInfo.check(data.schema) // 检查时间列名
    newTimeCol = newTimeCol.checkName(data.schema) // 检查新建的时间列名是否存在于

    this.timeColInfo.dataType match {
      case "string" =>
        val timeFormat = try {
          this.timeColInfo.timeFormat.get
        } catch {
          case _: Exception => throw new Exception("error105: 当时间列类型为string时需要有timeFormat")
        }
        val newName = newTimeCol.name
        data.withColumn(newName, unix_timestamp(col(timeColInfo.name), timeFormat))

      case "long" =>
        val utcFormat = timeColInfo.timeFormat.get
        utcFormat match {
          case "second" =>
            newTimeCol.update(timeColInfo.name, "long") // 此时不用转换，原有的就是恰好的
            data.withColumn(newTimeCol.name, col(timeColInfo.name).cast(LongType))
          case "millisecond" =>
            data.withColumn(newTimeCol.name, col(timeColInfo.name).cast(LongType)./(1000L))
        }
        newTimeCol.update(timeColInfo.name, "long") // 此时不用转换，原有的就是恰好的
        data.withColumn(newTimeCol.name, col(timeColInfo.name).cast(LongType))
    }
  }


  /**
    * 分箱的主要算法
    * ----
    * 将transform输出的以秒为单位的UTC时间数据进行分箱操作，输出分箱开始时间对应的UTC（以秒为单位）
    *
    * @param data 输入数据
    * @return 输出转换后的数据，同时如果是二级分箱[[binningTimeColBackUp]]的状态信息会变化
    */
  def run(data: DataFrame): DataFrame = {
    presentInfo.selfAdapt(this.timeColInfo, this.binningInfo) // 根据初始的时间列信息和分箱信息以及展示策略更新展示方式
    val transformDF = transform(data: DataFrame) // 将时间转换为秒的形式
    binningTimeCol = binningTimeCol.checkName(transformDF.schema) // 检查新建的时间列名是否存在于
    val binningColName = presentInfo.colToBePresented.name
    binningInfo match {
      case _: FixedLengthInfo =>
        transformDF.withColumn(binningColName, binningColUdf(col(newTimeCol.name)))
      case info: NaturalInfo =>
        if (!info.twoGrade)
          transformDF.withColumn(binningColName, binningColUdf(col(newTimeCol.name)))
        else {
          val binningExtraColName = presentInfo.extraColToBePresented.get.name
          transformDF
            .withColumn(binningColName, binningColUdf(col(newTimeCol.name)))
            .withColumn(binningExtraColName, binningColExtraUdf(col(newTimeCol.name)))
        }

    }
  }


  /**
    * udf函数 分箱和一级分箱会用到
    * 效果：将分箱信息直接显示成对应的类型
    */
  private def binningColUdf: UserDefinedFunction = {
    // 根据分箱区间的标签方式获得最终的展示数据
    val colToBePresented = presentInfo.colToBePresented
    colToBePresented.dataType match {
      case "string" =>
        NullableFunctions.udf(
          (timeStamp: Long) => {
            val binningStorage: BinningStorage = presentInfo.binningTagFormat match {
              case "byStart" => getTheBinningStartTime(timeStamp, this.binningInfo)
              case "byMiddle" => getTheBinningInterval(timeStamp)
              case "byInterval" => getTheMiddleTime(timeStamp)
            }

            new DateTime(binningStorage.binningPoint.point).toString(colToBePresented.timeFormat.get)
          })
      case "long" =>
        NullableFunctions.udf(
          (timeStamp: Long) => {
            val binningStorage: BinningStorage = presentInfo.binningTagFormat match {
              case "byStart" => getTheBinningStartTime(timeStamp, this.binningInfo)
              case "byMiddle" => getTheBinningInterval(timeStamp)
              case "byInterval" => getTheMiddleTime(timeStamp)
            }

            colToBePresented.timeFormat.get match {
              case "second" =>
                binningStorage.binningPoint.point
              case "millisecond" =>
                binningStorage.binningPoint.point * 1000
            }
          })
      case "timestamp" =>
        NullableFunctions.udf(
          (timeStamp: Long) => {
            val binningStorage: BinningStorage = presentInfo.binningTagFormat match {
              case "byStart" => getTheBinningStartTime(timeStamp, this.binningInfo)
              case "byMiddle" => getTheBinningInterval(timeStamp)
              case "byInterval" => getTheMiddleTime(timeStamp)
            }

            new Timestamp(binningStorage.binningPoint.point * 1000)
          })
      case "pasteString" =>
        NullableFunctions.udf(
          (timeStamp: Long) => {
            val binningStorage: BinningStorage = presentInfo.binningTagFormat match {
              case "byStart" => throw new Exception("error201，参数信息异常：展示信息为按起始时间标记分箱，类型不能为paste")
              case "byMiddle" => throw new Exception("error201，参数信息异常：展示信息为按中间时间标记分箱，类型不能为paste")
              case "byInterval" => getTheMiddleTime(timeStamp)
            }
            val start1 = new DateTime(binningStorage.binningPoint.point).toString(colToBePresented.timeFormat.get)
            val end1 = new DateTime(binningStorage.intervalBackUp.get.point).toString(colToBePresented.timeFormat.get)
            start1 + ", " + end1
          })

    }
  }


  /**
    * udf 对于二级分箱二级列的udf函数
    *
    * @return
    */
  private def binningColExtraUdf: UserDefinedFunction = { // 二级分箱才会用到
    // 根据分箱区间的标签方式获得最终的展示数据
    require(binningInfo.isInstanceOf[NaturalInfo] && binningInfo.asInstanceOf[NaturalInfo].twoGrade, "error201，参数信息异常：此时的分箱模式需要为二级分箱")
    val extraColToBePresented = try {
      presentInfo.extraColToBePresented.get
    } catch {
      case _: Exception => throw new Exception("error201，参数信息异常：在展示信息中没有找到二级分箱的信息设置")
    }
    extraColToBePresented.dataType match {
      case "string" =>
        NullableFunctions.udf(
          (timeStamp: Long) => {
            val binningStorage: BinningStorage = presentInfo.binningTagFormat match {
              case "byStart" => getTheBinningStartTime(timeStamp, this.binningInfo)
              case "byMiddle" => getTheBinningInterval(timeStamp)
              case "byInterval" => getTheMiddleTime(timeStamp)
            }

            new DateTime(binningStorage.binningPoint.pointBackUp.get).toString(extraColToBePresented.timeFormat.get)
          })
      case "long" =>
        NullableFunctions.udf(
          (timeStamp: Long) => {
            val binningStorage: BinningStorage = presentInfo.binningTagFormat match {
              case "byStart" => getTheBinningStartTime(timeStamp, this.binningInfo)
              case "byMiddle" => getTheBinningInterval(timeStamp)
              case "byInterval" => getTheMiddleTime(timeStamp)
            }

            extraColToBePresented.timeFormat.get match {
              case "second" =>
                binningStorage.binningPoint.pointBackUp.get
              case "millisecond" =>
                binningStorage.binningPoint.pointBackUp.get * 1000
            }
          })
      case "timestamp" =>
        NullableFunctions.udf(
          (timeStamp: Long) => {
            val binningStorage: BinningStorage = presentInfo.binningTagFormat match {
              case "byStart" => getTheBinningStartTime(timeStamp, this.binningInfo)
              case "byMiddle" => getTheBinningInterval(timeStamp)
              case "byInterval" => getTheMiddleTime(timeStamp)
            }

            new Timestamp(binningStorage.binningPoint.pointBackUp.get * 1000)
          })
      case "pasteString" =>
        NullableFunctions.udf(
          (timeStamp: Long) => {
            val binningStorage: BinningStorage = presentInfo.binningTagFormat match {
              case "byStart" => throw new Exception("error201，参数信息异常：展示信息为按起始时间标记分箱，类型不能为paste")
              case "byMiddle" => throw new Exception("error201，参数信息异常：展示信息为按中间时间标记分箱，类型不能为paste")
              case "byInterval" => getTheMiddleTime(timeStamp)
            }
            val start2 = new DateTime(binningStorage.binningPoint.pointBackUp).toString(extraColToBePresented.timeFormat.get)
            val end2 = new DateTime(binningStorage.intervalBackUp.get.pointBackUp).toString(extraColToBePresented.timeFormat.get)
            start2 + ", " + end2
          })
    }
  }


  /**
    * 具体的计算函数
    * ----
    * 分为三个:
    * [[getTheBinningStartTime]] [[getTheBinningInterval]] [[getTheMiddleTime]]
    * ----
    * 思想：
    * 1）不管展示的类型，一通算，如果二级分箱就将两个级别的时间全算出来输出，否则算出来一个输出
    * 2）后面udf利用时会结合展示形式提取算出来的数据，如果调用[[binningColUdf]]就提取数据中的第一级数据，
    * 如果调用[[binningColExtraUdf]]会提取第二级数据  --要求是二级分箱时才会调用，此时数据也才有二级数据，
    * 共同的控制条件是[[binningInfo]]
    * ----
    * 这样有点高内聚的感觉，不过这一段和上一段代码复用率低，后面看看有没有更好的设计
    */
  private def getTheBinningStartTime(timeStamp: Long, binningInfo: BinningInfo): BinningStorage = binningInfo match {
    case info: FixedLengthInfo =>
      val startTimeStamp = info.startTimeStamp
      val width = info.length
      BinningStorage(BinningPoint(math.floor((timeStamp - startTimeStamp) / width.toDouble).toInt * width + startTimeStamp))
    case info: NaturalInfo =>
      val slidingUnit: String = try {
        info.slidingUnit
      } catch {
        case _: Exception => throw new Exception("error201,参数信息异常: " +
          "没有找到精确平滑的单位信息")
      }
      val dateTime = new DateTime(timeStamp)
      if (!info.twoGrade) {
        val dt: DateTime = slidingUnit match {
          case "year" => dateTime.yearOfCentury().roundFloorCopy()
          case "season" => {
            val floorMonth = dateTime.monthOfYear().roundFloorCopy()
            val month = floorMonth.monthOfYear().get()
            floorMonth.plusMonths(-(month - 1) % 3)
          }
          case "month" => dateTime.monthOfYear().roundFloorCopy()
          case "week" => dateTime.weekOfWeekyear().roundFloorCopy()
          case "day" => dateTime.dayOfYear().roundFloorCopy()
          case "hour" => dateTime.hourOfDay().roundFloorCopy()
          case "minute" => dateTime.minuteOfDay().roundFloorCopy()
          case "second" => dateTime.secondOfDay().roundFloorCopy()
        }

        BinningStorage(BinningPoint(dt.getMillis / 1000))
      } else {
        val dt: DateTime = slidingUnit match {
          case "year" => dateTime.yearOfCentury().roundFloorCopy()
          case "season" => {
            val floorMonth = dateTime.monthOfYear().roundFloorCopy()
            val month = floorMonth.monthOfYear().get()
            floorMonth.plusMonths(-(month - 1) % 3)
          }
          case "month" => dateTime.monthOfYear().roundFloorCopy()
          case "week" => dateTime.weekOfWeekyear().roundFloorCopy()
          case "day" => dateTime.dayOfYear().roundFloorCopy()
          case "hour" => dateTime.hourOfDay().roundFloorCopy()
          case "minute" => dateTime.minuteOfDay().roundFloorCopy()
          case "second" => dateTime.secondOfDay().roundFloorCopy()
        }

        val minusMillis = dateTime.getMillis - dt.getMillis
        val resultBackEnd = getTheBinningStartTime(minusMillis, info.degrade()).binningPoint.point // info为stable类型
        BinningStorage(BinningPoint(dt.getMillis / 1000, Some(resultBackEnd)))
      }
  }

  private def getTheBinningInterval(timeStamp: Long): BinningStorage =
    binningInfo match {
      case info: FixedLengthInfo =>
        val startTimeStamp = info.startTimeStamp
        val width = info.length
        BinningStorage(BinningPoint(math.floor((timeStamp - startTimeStamp) / width.toDouble).toInt * width + startTimeStamp),
          Some(BinningPoint(math.ceil((timeStamp - startTimeStamp) / width.toDouble).toInt * width + startTimeStamp)))
      case info: NaturalInfo =>
        val slidingUnit: String = try {
          info.slidingUnit
        } catch {
          case _: Exception => throw new Exception("error201,参数信息异常: " +
            "没有找到精确平滑的单位信息")
        }
        val dateTime = new DateTime(timeStamp)
        if (!info.twoGrade) {
          val dt: DateTime = slidingUnit match {
            case "year" => dateTime.yearOfCentury().roundFloorCopy()
            case "season" => {
              val floorMonth = dateTime.monthOfYear().roundFloorCopy()
              val month = floorMonth.monthOfYear().get()
              floorMonth.plusMonths(-(month - 1) % 3)
            }
            case "month" => dateTime.monthOfYear().roundFloorCopy()
            case "week" => dateTime.weekOfWeekyear().roundFloorCopy()
            case "day" => dateTime.dayOfYear().roundFloorCopy()
            case "hour" => dateTime.hourOfDay().roundFloorCopy()
            case "minute" => dateTime.minuteOfDay().roundFloorCopy()
            case "second" => dateTime.secondOfDay().roundFloorCopy()
          }

          val interval = getInterval(dt, slidingUnit)
          BinningStorage(BinningPoint(interval._1), Some(BinningPoint(interval._2)))
        } else {
          val startTime = getTheBinningStartTime(timeStamp, this.binningInfo).binningPoint

          val firstTime = getInterval(new DateTime(startTime.point * 1000), slidingUnit)
          val extraTime = getInterval(new DateTime(startTime.pointBackUp.get * 1000), slidingUnit)

          BinningStorage(BinningPoint(firstTime._1, Some(extraTime._1)), Some(BinningPoint(firstTime._2, Some(extraTime._2))))
        }
    }


  private def getTheMiddleTime(timeStamp: Long): BinningStorage =
    binningInfo match {
      case info: FixedLengthInfo =>
        val startTimeStamp = info.startTimeStamp
        val width = info.length
        BinningStorage(BinningPoint(math.floor((timeStamp - startTimeStamp) / width.toDouble).toInt * width + width / 2 + startTimeStamp),
          None)
      case info: NaturalInfo =>
        val slidingUnit: String = try {
          info.slidingUnit
        } catch {
          case _: Exception => throw new Exception("error201,参数信息异常: " +
            "没有找到精确平滑的单位信息")
        }
        val dateTime = new DateTime(timeStamp)
        if (!info.twoGrade) {
          val dt: DateTime = slidingUnit match {
            case "year" => dateTime.yearOfCentury().roundFloorCopy()
            case "season" => {
              val floorMonth = dateTime.monthOfYear().roundFloorCopy()
              val month = floorMonth.monthOfYear().get()
              floorMonth.plusMonths(-(month - 1) % 3)
            }
            case "month" => dateTime.monthOfYear().roundFloorCopy()
            case "week" => dateTime.weekOfWeekyear().roundFloorCopy()
            case "day" => dateTime.dayOfYear().roundFloorCopy()
            case "hour" => dateTime.hourOfDay().roundFloorCopy()
            case "minute" => dateTime.minuteOfDay().roundFloorCopy()
            case "second" => dateTime.secondOfDay().roundFloorCopy()
          }

          val interval = getInterval(dt, slidingUnit)
          BinningStorage(BinningPoint((interval._1 + interval._2) / 2), None)
        } else {
          val startTime = getTheBinningStartTime(timeStamp, this.binningInfo).binningPoint

          val firstTime = getInterval(new DateTime(startTime.point * 1000), slidingUnit)
          val extraTime = getInterval(new DateTime(startTime.pointBackUp.get * 1000), slidingUnit)
          BinningStorage(BinningPoint((firstTime._1 + firstTime._2) / 2, Some((extraTime._1 + extraTime._2) / 2)))
        }
    }


  private def getInterval(startDt: DateTime, slidingUnit: String): (Long, Long) = {
    val endDt: DateTime = slidingUnit match {
      case "year" => startDt.plusYears(1)
      case "season" => startDt.plusMonths(3)
      case "month" => startDt.plusMonths(1)
      case "week" => startDt.plusWeeks(1)
      case "day" => startDt.plusDays(1)
      case "hour" => startDt.plusHours(1)
      case "minute" => startDt.plusMinutes(1)
      case "second" => startDt.plusSeconds(1)
    }
    (startDt.getMillis / 1000, endDt.getMillis / 1000)
  }


}
