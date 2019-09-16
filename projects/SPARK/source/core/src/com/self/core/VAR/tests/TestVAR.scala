//package com.self.core.VAR.tests
//
//import java.text.SimpleDateFormat
//
//import breeze.linalg.DenseMatrix
//import com.self.core.VAR.models._
//import com.self.core.baseApp.myAPP
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.functions.{col, lit, max, unix_timestamp}
//import org.apache.spark.sql.{DataFrame, Row}
//import org.apache.spark.sql.types._
//import org.joda.time.DateTime
//
//import scala.collection.mutable.ArrayBuffer
//
//object TestVAR extends myAPP {
//  def simulate(): DataFrame = {
//    val testData = TestData.canadaLaborData
//    import TestData.ArrayToMatrix
//    val lst = testData.toMatrixArrayByRow(84, 5)
//
//    val rdd = sc.parallelize(lst).map(Row.fromSeq(_))
//    sqlc.createDataFrame(rdd, StructType(Array(
//      StructField("time", StringType), StructField("x1", DoubleType),
//      StructField("x2", DoubleType), StructField("x3", DoubleType),
//      StructField("x4", DoubleType))))
//  }
//
//
//
//  override def run(): Unit = {
//    /** 原始模式 */
//    val rawDataDF = simulate()
//    rawDataDF.show()
//
//    /** 时间序列规整 --根据时间列信息将时间序列数据规整为标准的时间序列格式，并进行缺失值处理 */
//
//    // 需要的输入信息
//    val ipColInfo: Option[ColumnInfo] = None // id列名，如果没有就是None，有则是Some(ColumnInfo)
//    val timeColInfo: Option[TimeColInfo] = Some(TimeColInfo("dayTime", "StringType", Some("yyyy-MM"))) // 时间列信息
//    val intervalInfo: Option[IntervalInfo] = None
//
//    val frequencyInfo: FrequencyInfo = FrequencyInfo(intervalInfo.get: IntervalInfo, frequencyType: String, fixedLength: Some[Long], naturalUnit: Some[String])
//
//
//    /**
//      * 参数处理
//      */
//    // 1.id列
//    val hasIdCol = "true" // 是否有id列
//
//    // 2.时间序列信息
//    /** 分为三种：
//      * 1).输入时间列名                       || timeCol
//      * 2).输入字段类型                       || fieldFormat -- StringType, UTC, TimeStampType
//      * 3).输入时间列格式                     || timeFormat
//      * 4).输入时间序列起止时间                || startTime, endTime
//      * 5).请选择时间序列频率划分方式           || frequencyFormat
//      *     i)以固定长度时间划分                    -- fixedLength
//      *           输入长度                                   -- frequencyLength
//      *           输入长度单位                                -- unit
//      *     ii)以自然时间划分                       -- naturalLength
//      *           输入时间单位                                || unit
//      *           if(星期) 工作日 or 全周                         -- weekday, all
//      */
//    val timeCol = "dayTime"
//    val fieldFormat = "StringType"
//
//    val intervalType = "byHand" // 按最大最小时间平滑 --sliding
//
//
//    //    val frequencyFormat = "fixedLength" // fixedLength, naturalLength
//    val frequencyFormat = "naturalLength" // fixedLength, naturalLength
//
//    // 特征列
//    val featureCols: ArrayBuffer[(String, String)] = ArrayBuffer(("x", "int"), ("y", "int"), ("z", "int"))
//
//    val K = featureCols.length
//
//    val naFill: String = "linear" // "linear" "nearest" "next" "previous" "spline" "zero"
//
//
//    val col_type = rawDataDF.schema.fields.map(_.dataType)
//      .apply(rawDataDF.schema.fieldIndex(timeCol))
//
//    val newTimeCol = timeCol + "_new"
//    rawDataDF = fieldFormat match {
//      case "StringType" =>
//        if (col_type != StringType) {
//          throw new Exception(s"$timeCol 不是 StringType")
//        } else {
//          //          val timeFormat = "yyyy-MM-dd HH:mm:ss"
//          val timeFormat = "yyyy-MM"
//          rawDataDF.withColumn(newTimeCol, unix_timestamp(col(timeCol), timeFormat).*(1000))
//        }
//      case "TimeStampType" =>
//        if (col_type != TimestampType)
//          throw new Exception(s"$timeCol 不是 TimeStampType")
//        rawDataDF.withColumn(newTimeCol, col(timeCol).cast(LongType).*(1000))
//      case "UTC" => try {
//        rawDataDF.withColumn(newTimeCol, col(timeCol).cast(LongType).*(1000))
//      } catch {
//        case _: Exception =>
//          throw new Exception(s"$timeCol 不是 LongType")
//      }
//    }
//
//    rawDataDF.show()
//
//    val (startTimeStamp: Long, endTimeStamp: Long) = util.Try(intervalType match {
//      case "byHand" => {
//        //        val startTime = "2017-12-10 00:00:00"
//        //        val endTime = "2017-12-11 00:10:00"
//        val startTime = "2017-1"
//        val endTime = "2017-12"
//        val timeParser = new SimpleDateFormat("yyyy-MM")
//        (timeParser.parse(startTime).getTime, timeParser.parse(endTime).getTime)
//      }
//      case "sliding" => {
//        val minMax: DataFrame = rawDataDF.select(max(col(newTimeCol)).alias("max"), max(col(newTimeCol)).alias("max"))
//        minMax.rdd.map(r => (r.getAs[Long](0), r.getAs[Long](1))).collect().head
//      }
//    }) getOrElse (throw new Exception("类型不对"))
//
//
//    def getZeroValue(frequencyFormat: String, startTimeStamp: Long, endTimeStamp: Long)
//    : Array[Long] = frequencyFormat match {
//      case "fixedLength" => {
//        val frequencyLength = "1"
//        val unit = "minute"
//        val frequency: Int = {
//          util.Try(frequencyLength.toInt) getOrElse 1
//        } * (unit match {
//          case "week" => 604800000
//          case "day" => 86400000
//          case "hour" => 3600000
//          case "minute" => 60000
//          case "second" => 1000
//          case "microsecond" => 1
//        })
//        (startTimeStamp to endTimeStamp by frequency).toArray
//      }
//      case "naturalLength" => {
//        val unit = "month" // weekday, month, year, season
//        if (unit == "week") {
//          Array.range(startTimeStamp.toInt, endTimeStamp.toInt, 604800000).map(_.toLong)
//        } else if (unit == "weekday") {
//          Array.range(startTimeStamp.toInt, endTimeStamp.toInt, 604800000)
//            .flatMap(monday => Array.tabulate(5)(i => monday.toLong + i * 86400000))
//        } else {
//          val arr = ArrayBuffer.empty[Long]
//          var flashTime = startTimeStamp
//          var i = 1
//          val dt = new DateTime(startTimeStamp)
//          while (flashTime < endTimeStamp) {
//            arr += flashTime
//            unit match {
//              case "year" => {
//                flashTime = dt.plusYears(i).getMillis
//              }
//              case "month" => {
//                flashTime = dt.plusMonths(i).getMillis
//              }
//              case "season" => {
//                flashTime = dt.plusMonths(i * 3).getMillis
//              }
//              case _ => throw new Exception("您输入的模式不在year/month/season/week/weekday中")
//            }
//            i += 1
//          }
//          arr.toArray
//        }
//      }
//    }
//
//    var (idColName, idColType) = ("id_", "string_")
//
//    if(hasIdCol == "false"){
//      rawDataDF = rawDataDF.withColumn(idColName, lit("1"))
//    }else{
//      idColName = "id"
//      idColType = "string"
//    }
//
//    val rawRdd = rawDataDF.rdd.map(f = r => {
//      val id: String = idColType match {
//        case "int" => util.Try(r.getAs[Int](idColName).toString) getOrElse null
//        case "float" => util.Try(r.getAs[Float](idColName).toString) getOrElse null
//        case "double" => util.Try(r.getAs[Double](idColName).toString) getOrElse null
//        case "string" => util.Try(r.getAs[String](idColName).toString) getOrElse null
//      }
//
//      val arr = featureCols.map {
//        case (name, colType) => colType match {
//          case "int" => util.Try(r.getAs[Int](name).toDouble) getOrElse Double.NaN
//          case "float" => util.Try(r.getAs[Float](name).toDouble) getOrElse Double.NaN
//          case "double" => util.Try(r.getAs[Double](name)) getOrElse Double.NaN
//          case "string" => util.Try(r.getAs[String](name).toDouble) getOrElse Double.NaN
//        }
//      }
//
//      val time = util.Try(r.getAs[Long](newTimeCol)) getOrElse 0L // 时间
//
//      /** 时间转换 => 按给定的时间序列频率规则给出一个分界时间 & 还有和分界时间的时间差(当冲突时取最近时间的记录) */
//      val (roundTime: Long, modTime: Long) = frequencyFormat match {
//        case "fixedLength" => {
//          val frequencyLength = "1"
//          val unit = "minute"
//          val frequency: Int = {
//            util.Try(frequencyLength.toInt) getOrElse 1
//          } * (unit match {
//            case "week" => 604800000
//            case "day" => 86400000
//            case "hour" => 3600000
//            case "minute" => 60000
//            case "second" => 1000
//            case "microsecond" => 1
//          })
//          ((time - startTimeStamp) / frequency * frequency + startTimeStamp, (time - startTimeStamp) % frequency)
//        }
//        case "naturalLength" => {
//          val unit = "month" // weekday, month, year, season
//          val dt = new DateTime(time)
//          val roundTime = unit match {
//            case "year" => dt.yearOfCentury().roundFloorCopy().getMillis
//            case "month" => dt.monthOfYear().roundFloorCopy().getMillis
//            case "season" => {
//              val minus_month = (dt.getMonthOfYear - 1) % 3
//              dt.minusMonths(minus_month).monthOfYear().roundFloorCopy().getMillis
//            }
//            case "week" => (time - startTimeStamp) / 604800000
//            case "weekday" => {
//              val day_time = (time - startTimeStamp) / 86400000
//              val minus_day = day_time % 7 - 4
//              day_time - minus_day * 86400000
//            }
//          }
//          (roundTime, time - roundTime)
//        }
//      }
//      (id, (roundTime, modTime, arr.toArray))
//    })
//
//
//
//
//    val zeroValue: Map[Long, (Long, Array[Double])] = getZeroValue(frequencyFormat, startTimeStamp, endTimeStamp)
//      .map(roundTime => (roundTime, (Long.MaxValue, Array.fill(K)(Double.NaN)))).toMap
//
//
//    import com.self.core.VAR.models.MatrixNAFillImplicit._
//    val u = rawRdd.groupByKey().mapValues(iter => {
//      var buffer = zeroValue
//      val iterator = iter.toIterator
//      while (iterator.hasNext) {
//        val (roundTime, modTime, feature) = iterator.next()
//        if ((buffer contains roundTime) && buffer(roundTime)._1 > modTime)
//          buffer += (roundTime -> (modTime, feature))
//      }
//      buffer.toArray.sortBy(_._1).map(_._2._2).toDenseMatrix(false)
//    })
//
//    val s: RDD[(String, DenseMatrix[Double])] = u.mapValues(arr => {
//      arr.fillByCols(naFill)
//    })
//
//    s
//  }
//
//  val result: RDD[(String, DenseMatrix[Double])] = test()
//
//
//
//
//
//
//
//  }
//
//
//}
//
