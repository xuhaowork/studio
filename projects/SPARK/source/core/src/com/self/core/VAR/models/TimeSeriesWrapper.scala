//package com.self.core.VAR.models
//
//import java.text.SimpleDateFormat
//
//import breeze.linalg.DenseMatrix
//import com.self.core.VAR.utils.Tools
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions.{col, lit, max, min, unix_timestamp}
//import org.apache.spark.sql.types.{LongType, StringType}
//import org.joda
//import org.joda.time.{DateTime, Days, Interval}
//
//import scala.collection.mutable.ArrayBuffer
//
//
///**
//  * @param ipColInfo
//  * @param featureCols
//  * @param timeColInfo
//  * @param frequencyInfo
//  * @param naFill
//  */
//class TimeSeriesWrapper(var ipColInfo: Option[ColumnInfo],
//                        val featureCols: Array[(String, String)],
//                        var timeColInfo: TimeColInfo,
//                        var frequencyInfo: FrequencyInfo,
//                        val naFill: String) extends Serializable {
//  private def setIpColInfo(newIpColInfo: ColumnInfo): this.type = {
//    this.ipColInfo = Some(newIpColInfo)
//    this
//  }
//
//  private def setFrequencyInfo(newFrequencyInfo: FrequencyInfo): this.type = {
//    this.frequencyInfo = newFrequencyInfo
//    this
//  }
//
//  private def updateInterval(newIntervalInfo: IntervalInfo): this.type = {
//    this.frequencyInfo = this.frequencyInfo.updateInterval(newIntervalInfo)
//    this
//  }
//
//  def run(data: DataFrame) = {
//    /**
//      * Exception管理机制：
//      * 1：列名信息
//      * 101：id列信息转换出现错误
//      * 102：时间列信息转换出现错误
//      * 103：特征列信息转换出现错误
//      *
//      */
//    var df: DataFrame = data
//    df = try {
//      if (ipColInfo.isEmpty) {
//        setIpColInfo(ColumnInfo("id_", "string"))
//        val colInfo = this.ipColInfo.get
//        df.withColumn(colInfo.name, lit("1"))
//        df.withColumn(colInfo.name, lit("1"))
//      } else {
//        val colInfo = this.ipColInfo.get
//        df.withColumn(colInfo.name, col(colInfo.name).cast(StringType))
//      }
//    } catch {
//      case _: Exception => throw new Exception("101：id列信息转换出现错误，可能的原因是：您输入的id列不存在或不能直接转为String")
//    }
//
//    val (fieldName, fieldFormat) = (this.timeColInfo.fieldType, this.timeColInfo.name)
//    df = try {
//      fieldFormat match {
//        case "StringType" =>
//          val timeFormat = this.timeColInfo.timeFormat.get
//          df.withColumn(fieldName, unix_timestamp(col(fieldName), timeFormat).*(1000))
//        case "TimeStampType" =>
//          df.withColumn(fieldName, col(fieldName).cast(LongType).*(1000))
//        case "UTC" =>
//          df.withColumn(fieldName, col(fieldName).cast(LongType).*(1000))
//      }
//    } catch {
//      case _: Exception => throw new Exception("102：时间列信息转换出现错误")
//    }
//
//    if (this.frequencyInfo.intervalInfo.isEmpty) {
//      val intervalSlidingUnit = this.frequencyInfo.intervalSlidingUnit.getOrElse("")
//      val (startTimeStamp: Long, endTimeStamp: Long) = {
//        val minMax: DataFrame = df.select(min(col(fieldName)).alias("max"), max(col(fieldName)).alias("max"))
//        minMax.rdd.map(r => (r.getAs[Long](0), r.getAs[Long](1))).collect().head
//      }
//      val interval = IntervalInfo(Tools.slidingTimeByNatural(startTimeStamp, intervalSlidingUnit),
//        Tools.slidingTimeByNatural(endTimeStamp, intervalSlidingUnit))
//      updateInterval(interval)
//    }
//
//    val rawRdd = df.rdd.map(r => {
//      val id: String = this.ipColInfo.get.dataType match {
//        case "int" => util.Try(r.getAs[Int](this.ipColInfo.get.name).toString) getOrElse null
//        case "float" => util.Try(r.getAs[Float](this.ipColInfo.get.name).toString) getOrElse null
//        case "double" => util.Try(r.getAs[Double](this.ipColInfo.get.name).toString) getOrElse null
//        case "string" => util.Try(r.getAs[String](this.ipColInfo.get.name).toString) getOrElse null
//      }
//
//      val arr = this.featureCols.map {
//        case (name, colType) => colType match {
//          case "int" => util.Try(r.getAs[Int](name).toDouble) getOrElse Double.NaN
//          case "float" => util.Try(r.getAs[Float](name).toDouble) getOrElse Double.NaN
//          case "double" => util.Try(r.getAs[Double](name)) getOrElse Double.NaN
//          case "string" => util.Try(r.getAs[String](name).toDouble) getOrElse Double.NaN
//        }
//      }
//
//      val time: Long = util.Try(r.getAs[Long](this.timeColInfo.name)) getOrElse 0L // 时间
//
//
//      //      /** 时间转换 => 按给定的时间序列频率规则给出一个分界时间 & 还有和分界时间的时间差(当冲突时取最近时间的记录) */
//      //      val (roundTime: Long, modTime: Long) = frequencyFormat match {
//      //        case "fixedLength" => {
//      //          val frequencyLength = "1"
//      //          val unit = "minute"
//      //          val frequency: Int = {
//      //            util.Try(frequencyLength.toInt) getOrElse 1
//      //          } * (unit match {
//      //            case "week" => 604800000
//      //            case "day" => 86400000
//      //            case "hour" => 3600000
//      //            case "minute" => 60000
//      //            case "second" => 1000
//      //            case "microsecond" => 1
//      //          })
//      //          ((time - startTimeStamp) / frequency * frequency + startTimeStamp, (time - startTimeStamp) % frequency)
//      //        }
//      //        case "naturalLength" => {
//      //          val unit = "month" // weekday, month, year, season
//      //          val dt = new DateTime(time)
//      //          val roundTime = unit match {
//      //            case "year" => dt.yearOfCentury().roundFloorCopy().getMillis
//      //            case "month" => dt.monthOfYear().roundFloorCopy().getMillis
//      //            case "season" => {
//      //              val minus_month = (dt.getMonthOfYear - 1) % 3
//      //              dt.minusMonths(minus_month).monthOfYear().roundFloorCopy().getMillis
//      //            }
//      //            case "week" => (time - startTimeStamp) / 604800000
//      //            case "weekday" => {
//      //              val day_time = (time - startTimeStamp) / 86400000
//      //              val minus_day = day_time % 7 - 4
//      //              day_time - minus_day * 86400000
//      //            }
//      //          }
//      //          (roundTime, time - roundTime)
//      //        }
//      //      }
//      //      (id, (roundTime, modTime, arr.toArray))
//      //    })
//
//
////    import com.self.core.VAR.models.MatrixNAFillImplicit._
////    val u = rawRdd.groupByKey().mapValues(iter => {
////      var buffer = zeroValue
////      val iterator = iter.toIterator
////      while (iterator.hasNext) {
////        val (roundTime, modTime, feature) = iterator.next()
////        if ((buffer contains roundTime) && buffer(roundTime)._1 > modTime)
////          buffer += (roundTime -> (modTime, feature))
////      }
////      buffer.toArray.sortBy(_._1).map(_._2._2).toDenseMatrix(false)
////    })
////
////    val s: RDD[(String, DenseMatrix[Double])] = u.mapValues(arr => {
////      arr.fillByCols(naFill)
////    })
//
//
//
//
//      //        (unit match {
//      //          case "week" => 604800000
//      //          case "day" => 86400000
//      //          case "hour" => 3600000
//      //          case "minute" => 60000
//      //          case "second" => 1000
//      //          case "microsecond" => 1
//      //        })
//
//
//  def slidingByNaturalLength(time: Long, startTimeStamp: Long, unit: String) = {
//    val dt = new DateTime(time)
//    val startDt = new DateTime(startTimeStamp)
//    val iv: Interval = new Interval(startDt, dt)
//
//
//    unit match {
//      case "year" => {
//        val roundTime = dt.yearOfCentury().roundFloorCopy().getMillis
//        val index = dt.getYear - startDt.getYear
//        SlidingTime(roundTime, time - roundTime, index)
//      }
//      case "month" => {
//        val roundTime = dt.monthOfYear().roundFloorCopy().getMillis
//        val index = (startDt.getYear - dt.getYear) * 12 + (startDt.getMonthOfYear - dt.getMonthOfYear)
//        SlidingTime(roundTime, time - roundTime, index)
//      }
//      case "season" => {
//        val minus_month = (dt.getMonthOfYear - 1) % 3
//        val roundTime = dt.minusMonths(minus_month).monthOfYear().roundFloorCopy().getMillis
//        val index = (startDt.getYear - dt.getYear) * 12 + (startDt.getMonthOfYear - dt.getMonthOfYear)
//        SlidingTime(roundTime, time - roundTime, index / 3)
//      }
//      case "week" => slidingByFixedLength(time, startTimeStamp, 604800000)
//      case "weekday" => {
//        val day_time = (time - startTimeStamp) / 86400000 // 要保证startTimeStamp是Monday
//        val minus_day = day_time % 7 - 4
//        day_time - minus_day * 86400000
//      }
//
//
//
//    }
//
//
//  def slidingByFixedLength(time: Long, startTimeStamp: Long, frequency: Long): SlidingTime = {
//    val index = (time - startTimeStamp) / frequency
//    val roundTime = index * frequency + startTimeStamp
//    SlidingTime(roundTime, time - roundTime, index)
//  }
//
//  def slidingWithAllInfo(time: Long) = {
//    /** 时间转换 => 按给定的时间序列频率规则给出一个分界时间 & 还有和分界时间的时间差(当冲突时取最近时间的记录) */
//    val frequencyType = this.frequencyInfo.frequencyType
//    val startTimeStamp = this.frequencyInfo.intervalInfo.get.start
//    frequencyType match {
//      case "fixedLength" => {
//        val frequency = this.frequencyInfo.fixedLength.get
//        slidingByFixedLength(time, startTimeStamp, frequency)
//      }
//      case "naturalLength" => {
//        val unit = this.frequencyInfo.naturalUnit.get // weekday, month, year, season
//        val dt = new DateTime(time)
//        unit match {
//          case "year" => dt.yearOfCentury().roundFloorCopy().getMillis
//          case "month" => dt.monthOfYear().roundFloorCopy().getMillis
//          case "season" => {
//            val minus_month = (dt.getMonthOfYear - 1) % 3
//            dt.minusMonths(minus_month).monthOfYear().roundFloorCopy().getMillis
//          }
//          case "week" => slidingByFixedLength(time, startTimeStamp, 604800000)
//          case "weekday" => {
//            val day_time = (time - startTimeStamp) / 86400000
//            val minus_day = day_time % 7 - 4
//            day_time - minus_day * 86400000
//          }
//          case "day" => slidingByFixedLength(time, startTimeStamp, 86400000)
//          case "hour" => slidingByFixedLength(time, startTimeStamp, 3600000)
//          case "minute" => slidingByFixedLength(time, startTimeStamp, 60000)
//          case "second" => slidingByFixedLength(time, startTimeStamp, 1000)
//        }
//        (roundTime, time - roundTime)
//      }
//    }
//    }
//    (id, (roundTime, modTime, arr.toArray))
//  })
//
//
//
//
//
//}
//
//
//}
