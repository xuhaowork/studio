package com.self.core.VectorAutoRegression.utils

import java.sql.Timestamp

import org.apache.spark.SparkException
import org.joda.time.{DateTime, Period, PeriodType}

object ToolsForTimeSeriesWarp extends Serializable {
  /** 时间分箱 由Timestamp => Timestamp */
  def binning(time: Timestamp, phase: Long, window: Int, unit: String): Timestamp = {
    /** 先将相位和时间都以Long类型时间的数值变为默认时区下[[DateTime]]类型的时间 */
    val phase_dt = new DateTime(phase) // 默认时区即为相位所在时区 --如果是绝对时间相位为默认时区，否则相位需要为UTC时间
    val value_dt = new DateTime(time.getTime)
    val period: Period = new Period(phase_dt, value_dt) // 构建可以用于获得任意单位相对时差的Period

    /** 分别获的分箱时间和分箱时间的有边界 */
    unit match {
      case "year" =>
        val periodForSpecialUnit: Period = new Period(phase_dt, value_dt, PeriodType.years())

        val result = try {
          Array(
            period.getMonths,
            period.getDays,
            period.getHours,
            period.getMinutes,
            period.getSeconds,
            period.getMillis
          ).foldLeft(0.0)((b, minus) => if (b != 0) b else {
            if (minus < 0) -1.0 else 0.0
          })
        } catch {
          case e: Exception => throw new SparkException(s"分箱计算过程中的取整计算失败，具体异常为${e.getMessage}")
        } // 用来处理取整问题

        val value = phase_dt.plusYears(
          scala.math.floor((periodForSpecialUnit.getYears + result) / window).toInt * window.toInt
        ).yearOfCentury().roundFloorCopy().getMillis
        new Timestamp(value)

      case "month" =>
        val periodForSpecialUnit: Period = new Period(phase_dt, value_dt, PeriodType.months())

        val result = try {
          Array(
            period.getDays,
            period.getHours,
            period.getMinutes,
            period.getSeconds,
            period.getMillis
          ).foldLeft(0.0)((b, minus) => if (b != 0) b else {
            if (minus < 0) -1.0 else 0.0
          })
        } catch {
          case e: Exception => throw new SparkException(s"分箱计算过程中的取整计算失败，具体异常为${e.getMessage}")
        }

        val value = phase_dt.plusMonths(
          scala.math.floor((periodForSpecialUnit.getMonths + result) / window).toInt * window.toInt
        ).monthOfYear().roundFloorCopy().getMillis
        new Timestamp(value)

      case "week" =>
        val periodForSpecialUnit: Period = new Period(phase_dt, value_dt, PeriodType.weeks())

        val result = try {
          Array(
            period.getDays,
            period.getHours,
            period.getMinutes,
            period.getSeconds,
            period.getMillis
          ).foldLeft(0.0)((b, minus) => if (b != 0) b else {
            if (minus < 0) -1.0 else 0.0
          })
        } catch {
          case e: Exception => throw new SparkException(s"分箱计算过程中的取整计算失败，具体异常为${e.getMessage}")
        }

        val value = phase_dt.plusWeeks(
          scala.math.floor((periodForSpecialUnit.getWeeks + result) / window).toInt * window.toInt
        ).weekOfWeekyear().roundFloorCopy().getMillis
        new Timestamp(value)

      case "day" =>
        val length = window * 86400000
        val milliValue = scala.math.floor((value_dt.getMillis - phase_dt.getMillis).toDouble / length).toLong * length +
          phase_dt.getMillis
        new Timestamp(milliValue)

      case "hour" =>
        val length = window * 3600000
        val milliValue = scala.math.floor((value_dt.getMillis - phase_dt.getMillis).toDouble / length).toLong * length +
          phase_dt.getMillis
        new Timestamp(milliValue)

      case "minute" =>
        val length = window * 60000
        val milliValue = scala.math.floor((value_dt.getMillis - phase_dt.getMillis).toDouble / length).toLong * length +
          phase_dt.getMillis

        new Timestamp(milliValue)

      case "second" =>
        val length = window * 1000
        val milliValue = scala.math.floor((value_dt.getMillis - phase_dt.getMillis).toDouble / length).toLong * length +
          phase_dt.getMillis
        new Timestamp(milliValue)

      case "millisecond" => // 不会有取整问题
        val length = window
        val milliValue = scala.math.floor((value_dt.getMillis - phase_dt.getMillis).toDouble / length).toLong * length +
          phase_dt.getMillis
        new Timestamp(milliValue)

      case _ =>
        throw new Exception("您输入的时间单位有误: 目前只能是year/month/week/hour/minute/second/millisecond之一")
    }
  }


  /**
    * 根据分箱信息将时间分箱并获得所在箱子的id
    * ----
    *
    * @param time
    * @param phase
    * @param window
    * @param unit
    */
  def binningForId(time: Timestamp, phase: Long, window: Int, unit: String): Long = {
    /** 先将相位和时间都以Long类型时间的数值变为默认时区下[[DateTime]]类型的时间 */
    val phase_dt = new DateTime(phase) // 默认时区即为相位所在时区 --如果是绝对时间相位为默认时区，否则相位需要为UTC时间
    val value_dt = new DateTime(time)
    val period: Period = new Period(phase_dt, value_dt) // 构建可以用于获得任意单位相对时差的Period

    /** 分别获的分箱时间和分箱时间的有边界 */
    unit match {
      case "year" =>
        val periodForSpecialUnit: Period = new Period(phase_dt, value_dt, PeriodType.years())
        val result = try {
          Array(
            period.getMonths,
            period.getDays,
            period.getHours,
            period.getMinutes,
            period.getSeconds,
            period.getMillis
          ).foldLeft(0.0)((b, minus) => if (b != 0) b else {
            if (minus < 0) -1.0 else 0.0
          })
        } catch {
          case e: Exception => throw new SparkException(s"分箱计算过程中的取整计算失败，具体异常为${e.getMessage}")
        } // 用来处理取整问题
        scala.math.floor((periodForSpecialUnit.getYears + result) / window).toLong

      case "month" =>
        val periodForSpecialUnit: Period = new Period(phase_dt, value_dt, PeriodType.months())
        val result = try {
          Array(
            period.getDays,
            period.getHours,
            period.getMinutes,
            period.getSeconds,
            period.getMillis
          ).foldLeft(0.0)((b, minus) => if (b != 0) b else {
            if (minus < 0) -1.0 else 0.0
          })
        } catch {
          case e: Exception => throw new SparkException(s"分箱计算过程中的取整计算失败，具体异常为${e.getMessage}")
        }

        scala.math.floor((periodForSpecialUnit.getMonths + result) / window).toLong

      case "week" =>
        val periodForSpecialUnit: Period = new Period(phase_dt, value_dt, PeriodType.weeks())
        val result = try {
          Array(
            period.getDays,
            period.getHours,
            period.getMinutes,
            period.getSeconds,
            period.getMillis
          ).foldLeft(0.0)((b, minus) => if (b != 0) b else {
            if (minus < 0) -1.0 else 0.0
          })
        } catch {
          case e: Exception => throw new SparkException(s"分箱计算过程中的取整计算失败，具体异常为${e.getMessage}")
        }

        scala.math.floor((periodForSpecialUnit.getWeeks + result) / window).toLong

      case "day" =>
        val length = window * 86400000
        scala.math.floor((value_dt.getMillis - phase_dt.getMillis).toDouble / length).toLong

      case "hour" =>
        val length = window * 3600000
        scala.math.floor((value_dt.getMillis - phase_dt.getMillis).toDouble / length).toLong

      case "minute" =>
        val length = window * 60000
        scala.math.floor((value_dt.getMillis - phase_dt.getMillis).toDouble / length).toLong

      case "second" =>
        val length = window * 1000
        scala.math.floor((value_dt.getMillis - phase_dt.getMillis).toDouble / length).toLong

      case "millisecond" => // 不会有取整问题
        val length = window
        scala.math.floor((value_dt.getMillis - phase_dt.getMillis).toDouble / length).toLong

      case _ =>
        throw new Exception("您输入的时间单位有误: 目前只能是year/month/week/hour/minute/second/millisecond之一")
    }
  }


  /** 根据初始相位信息和分箱信息将id转换为分箱后的Timestamp */
  def transformBinningIdToTime(id: Long, phase: Long, window: Int, unit: String): Timestamp = {
    /** 先将相位和时间都以Long类型时间的数值变为默认时区下[[DateTime]]类型的时间 */
    val phase_dt = new DateTime(phase) // 默认时区即为相位所在时区 --如果是绝对时间相位为默认时区，否则相位需要为UTC时间

    /** 分别获的分箱时间和分箱时间的有边界 */
    val dt = unit match {
      case "year" =>
        phase_dt.plusYears(id.toInt * window).yearOfCentury().roundFloorCopy()

      case "month" =>
        phase_dt.plusMonths(id.toInt * window).monthOfYear().roundFloorCopy()

      case "week" =>
        phase_dt.plusWeeks(id.toInt * window).weekOfWeekyear().roundFloorCopy()

      case "day" =>
        val length = window.toLong * 86400000
        new DateTime(id * length + phase_dt.getMillis)

      case "hour" =>
        val length = window.toLong * 3600000
        new DateTime(id * length + phase_dt.getMillis)

      case "minute" =>
        val length = window.toLong * 60000
        new DateTime(id * length + phase_dt.getMillis)

      case "second" =>
        val length = window.toLong * 1000
        new DateTime(id * length + phase_dt.getMillis)

      case "millisecond" => // 不会有取整问题
        val length = window.toLong
        new DateTime(id * length + phase_dt.getMillis)

      case _ =>
        throw new Exception("您输入的时间单位有误: 目前只能是year/month/week/hour/minute/second/millisecond之一")
    }

    new java.sql.Timestamp(dt.getMillis)
  }


}
