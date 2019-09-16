package com.self.core.VAR.tests

import java.text.SimpleDateFormat

import breeze.linalg.DenseMatrix
import com.self.core.baseApp.myAPP
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, max, unix_timestamp}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import scala.collection.mutable.ArrayBuffer

/**
  * 测试时间序列规整算子
  */

object TestTimeWrapping extends myAPP{
  def test(): RDD[(String, DenseMatrix[Double])] = {
    /**
      * 模拟数据
      */
    //        val arr = List(
    //          Array("1", "2017-12-10 00:00:00", 123, -150, -150),
    //          Array("1", "2017-12-10 00:01:01", 9, -150, -150),
    //          Array("1", "2017-12-10 00:03:00", 19, -10, -150),
    //          Array("1", "2017-12-10 00:01:01", 19, -150, -150),
    //          Array("1", "2017-12-10 00:02:00", 199, 50, -150),
    //          Array("1", "2017-12-10 00:04:00", 199, -150, -150),
    //          Array("1", "2017-12-10 00:09:00", 199, 0, 0),
    //          Array("2", "2017-12-10 00:07:00", 99, 0, -150),
    //          Array("2", null, 199, -150, -150),
    //          Array("2", "2017-12-10 00:01:00", 1, -50, -15),
    //          Array("2", "2017-12-10 00:02:02", 90, -0, -1),
    //          Array("2", "2017-12-10 00:02:02", 19, -50, -15))

    val arr2 = List(
      Array("1", "2017-01", 123, -150, -150),
      Array("1", "2017-02", 9, -150, -150),
      Array("1", "2017-02", 19, -10, -150),
      Array("1", "2017-03", 19, -150, -150),
      Array("1", "2017-07", 1, 50, -150),
      Array("1", "2017-05", 199, -8, -150),
      Array("1", "2017-07", 120, 7, 0),
      Array("1", "2017-08", 199, 0, 197),
      Array("1", "2017-09", -200, 100, 50),
      Array("1", "2017-10", 60, 50, 0),
      Array("1", "2017-11", 199, 98, 0),
      Array("1", "2017-12", 100, 0, 10),
      Array("2", "2017-01", 99, -70, -150),
      Array("2", null, 199, -150, -150),
      Array("2", "2017-02", 1, -50, -15),
      Array("2", "2017-05", 90, -0, -1),
      Array("2", "2017-06", 19, -50, -15),
      Array("2", null, 199, -150, -150),
      Array("2", "2017-03", 1, -50, -15),
      Array("2", "2017-07", 90, -0, -1),
      Array("2", "2017-08", 19, -50, -15),
      Array("2", "2017-09", 1, -50, -15),
      Array("2", "2017-10", 90, -0, -1),
      Array("2", "2017-11", 19, -50, -15)
    )

    val rdd = sc.parallelize(arr2).map(Row.fromSeq(_))
    var rawDataDF = sqlc.createDataFrame(rdd, StructType(
      Array(StructField("id", StringType),
        StructField("dayTime", StringType),
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("z", IntegerType))))



    /**
      * 参数处理
      */
    // 1.id列
    val hasIdCol = "true" // 是否有id列

    // 2.时间序列信息
    /** 分为三种：
      * 1).输入时间列名                       || timeCol
      * 2).输入字段类型                       || fieldFormat -- StringType, UTC, TimeStampType
      * 3).输入时间列格式                     || timeFormat
      * 4).输入时间序列起止时间                || startTime, endTime
      * 5).请选择时间序列频率划分方式           || frequencyFormat
      *     i)以固定长度时间划分                    -- fixedLength
      *           输入长度                                   -- frequencyLength
      *           输入长度单位                                -- unit
      *     ii)以自然时间划分                       -- naturalLength
      *           输入时间单位                                || unit
      *           if(星期) 工作日 or 全周                         -- weekday, all
      */
    val timeCol = "dayTime"
    val fieldFormat = "StringType"

    val intervalType = "byHand" // 按最大最小时间平滑 --sliding


    //    val frequencyFormat = "fixedLength" // fixedLength, naturalLength
    val frequencyFormat = "naturalLength" // fixedLength, naturalLength

    // 特征列
    val featureCols: ArrayBuffer[(String, String)] = ArrayBuffer(("x", "int"), ("y", "int"), ("z", "int"))

    val K = featureCols.length

    val naFill = "linear" // "linear" "nearest" "next" "previous" "spline" "zero"


    val col_type = rawDataDF.schema.fields.map(_.dataType)
      .apply(rawDataDF.schema.fieldIndex(timeCol))

    val newTimeCol = timeCol + "_new"
    rawDataDF = fieldFormat match {
      case "StringType" =>
        if (col_type != StringType) {
          throw new Exception(s"$timeCol 不是 StringType")
        } else {
          //          val timeFormat = "yyyy-MM-dd HH:mm:ss"
          val timeFormat = "yyyy-MM"
          rawDataDF.withColumn(newTimeCol, unix_timestamp(col(timeCol), timeFormat).*(1000))
        }
      case "TimeStampType" =>
        if (col_type != TimestampType)
          throw new Exception(s"$timeCol 不是 TimeStampType")
        rawDataDF.withColumn(newTimeCol, col(timeCol).cast(LongType).*(1000))
      case "UTC" => try {
        rawDataDF.withColumn(newTimeCol, col(timeCol).cast(LongType).*(1000))
      } catch {
        case _: Exception =>
          throw new Exception(s"$timeCol 不是 LongType")
      }
    }

    rawDataDF.show()

    val (startTimeStamp: Long, endTimeStamp: Long) = util.Try(intervalType match {
      case "byHand" => {
        //        val startTime = "2017-12-10 00:00:00"
        //        val endTime = "2017-12-11 00:10:00"
        val startTime = "2017-1"
        val endTime = "2017-12"
        val timeParser = new SimpleDateFormat("yyyy-MM")
        (timeParser.parse(startTime).getTime, timeParser.parse(endTime).getTime)
      }
      case "sliding" => {
        val minMax: DataFrame = rawDataDF.select(max(col(newTimeCol)).alias("max"), max(col(newTimeCol)).alias("max"))
        minMax.rdd.map(r => (r.getAs[Long](0), r.getAs[Long](1))).collect().head
      }
    }) getOrElse (throw new Exception("类型不对"))


    def getZeroValue(frequencyFormat: String, startTimeStamp: Long, endTimeStamp: Long)
    : Array[Long] = frequencyFormat match {
      case "fixedLength" => {
        val frequencyLength = "1"
        val unit = "minute"
        val frequency: Int = {
          util.Try(frequencyLength.toInt) getOrElse 1
        } * (unit match {
          case "week" => 604800000
          case "day" => 86400000
          case "hour" => 3600000
          case "minute" => 60000
          case "second" => 1000
          case "microsecond" => 1
        })
        (startTimeStamp to endTimeStamp by frequency).toArray
      }
      case "naturalLength" => {
        val unit = "month" // weekday, month, year, season
        if (unit == "week") {
          Array.range(startTimeStamp.toInt, endTimeStamp.toInt, 604800000).map(_.toLong)
        } else if (unit == "weekday") {
          Array.range(startTimeStamp.toInt, endTimeStamp.toInt, 604800000)
            .flatMap(monday => Array.tabulate(5)(i => monday.toLong + i * 86400000))
        } else {
          val arr = ArrayBuffer.empty[Long]
          var flashTime = startTimeStamp
          var i = 1
          val dt = new DateTime(startTimeStamp)
          while (flashTime < endTimeStamp) {
            arr += flashTime
            unit match {
              case "year" => {
                flashTime = dt.plusYears(i).getMillis
              }
              case "month" => {
                flashTime = dt.plusMonths(i).getMillis
              }
              case "season" => {
                flashTime = dt.plusMonths(i * 3).getMillis
              }
              case _ => throw new Exception("您输入的模式不在year/month/season/week/weekday中")
            }
            i += 1
          }
          arr.toArray
        }
      }
    }

    var (idColName, idColType) = ("id_", "string_")

    if(hasIdCol == "false"){
      rawDataDF = rawDataDF.withColumn(idColName, lit("1"))
    }else{
      idColName = "id"
      idColType = "string"
    }

    val rawRdd = rawDataDF.rdd.map(f = r => {
      val id: String = idColType match {
        case "int" => util.Try(r.getAs[Int](idColName).toString) getOrElse null
        case "float" => util.Try(r.getAs[Float](idColName).toString) getOrElse null
        case "double" => util.Try(r.getAs[Double](idColName).toString) getOrElse null
        case "string" => util.Try(r.getAs[String](idColName).toString) getOrElse null
      }

      val arr = featureCols.map {
        case (name, colType) => colType match {
          case "int" => util.Try(r.getAs[Int](name).toDouble) getOrElse Double.NaN
          case "float" => util.Try(r.getAs[Float](name).toDouble) getOrElse Double.NaN
          case "double" => util.Try(r.getAs[Double](name)) getOrElse Double.NaN
          case "string" => util.Try(r.getAs[String](name).toDouble) getOrElse Double.NaN
        }
      }

      val time = util.Try(r.getAs[Long](newTimeCol)) getOrElse 0L // 时间

      /** 时间转换 => 按给定的时间序列频率规则给出一个分界时间 & 还有和分界时间的时间差(当冲突时取最近时间的记录) */
      val (roundTime: Long, modTime: Long) = frequencyFormat match {
        case "fixedLength" => {
          val frequencyLength = "1"
          val unit = "minute"
          val frequency: Int = {
            util.Try(frequencyLength.toInt) getOrElse 1
          } * (unit match {
            case "week" => 604800000
            case "day" => 86400000
            case "hour" => 3600000
            case "minute" => 60000
            case "second" => 1000
            case "microsecond" => 1
          })
          ((time - startTimeStamp) / frequency * frequency + startTimeStamp, (time - startTimeStamp) % frequency)
        }
        case "naturalLength" => {
          val unit = "month" // weekday, month, year, season
          val dt = new DateTime(time)
          val roundTime = unit match {
            case "year" => dt.yearOfCentury().roundFloorCopy().getMillis
            case "month" => dt.monthOfYear().roundFloorCopy().getMillis
            case "season" => {
              val minus_month = (dt.getMonthOfYear - 1) % 3
              dt.minusMonths(minus_month).monthOfYear().roundFloorCopy().getMillis
            }
            case "week" => (time - startTimeStamp) / 604800000
            case "weekday" => {
              val day_time = (time - startTimeStamp) / 86400000
              val minus_day = day_time % 7 - 4
              day_time - minus_day * 86400000
            }
          }
          (roundTime, time - roundTime)
        }
      }
      (id, (roundTime, modTime, arr.toArray))
    })




    val zeroValue: Map[Long, (Long, Array[Double])] = getZeroValue(frequencyFormat, startTimeStamp, endTimeStamp)
      .map(roundTime => (roundTime, (Long.MaxValue, Array.fill(K)(Double.NaN)))).toMap


    import com.self.core.VAR.models.MatrixNAFillImplicit._
    val u = rawRdd.groupByKey().mapValues(iter => {
      var buffer = zeroValue
      val iterator = iter.toIterator
      while (iterator.hasNext) {
        val (roundTime, modTime, feature) = iterator.next()
        if ((buffer contains roundTime) && buffer(roundTime)._1 > modTime)
          buffer += (roundTime -> (modTime, feature))
      }
      buffer.toArray.sortBy(_._1).map(_._2._2).toDenseMatrix(false)
    })

    val s: RDD[(String, DenseMatrix[Double])] = u.mapValues(arr => {
      arr.fillByCols(naFill)
    })

    s
  }

  val result: RDD[(String, DenseMatrix[Double])] = test()

  override def run(): Unit = {
    result.foreach(println)
  }
}
