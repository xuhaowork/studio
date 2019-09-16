package cn.datashoe.examples

import org.apache.spark.sql.DataFrame

object A {
  import org.apache.spark.sql.SparkSession

  val spark: SparkSession = null

  def test(): DataFrame = {
    import java.text.SimpleDateFormat
//    import com.sefon.spark.diver.MLlibDriver
    import org.apache.spark.rdd.RDD
    import org.apache.spark.sql.DataFrame
    import spark.implicits._

    import scala.collection.mutable
    import scala.math.{abs, max, min}
    import scala.util.matching.Regex


    /**
      * 瓦斯数据
      *
      * @param deviceId       设备ID
      * @param time           时间
      * @param unitName       煤矿名称
      * @param unitId         煤矿ID
      * @param gasConsistency 瓦斯值
      */
    case class GasValues(
                          deviceId: Int,
                          time: Long,
                          unitName: String,
                          unitId: Int,
                          gasConsistency: Double,
                          period: Long = 0L
                        ) {
      override def toString: String =
        Array(deviceId, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time), unitName, unitId, gasConsistency)
          .mkString("GasValues(", ",", ")")

      def updateTime(newTime: Long): GasValues = {
        GasValues(deviceId, newTime, unitName, unitId, gasConsistency)
      }

      def updateValue(newGasConsistency: Double): GasValues = {
        GasValues(deviceId, time, unitName, unitId, newGasConsistency)
      }

      def updatePeriod(newPeriod: Long): GasValues = {
        GasValues(deviceId, time, unitName, unitId, gasConsistency, newPeriod)
      }

    }


    /**
      * 时间序列处理的工具类
      * ----
      * 重分区: 将同一个设备的数据分到同一个分区并且进行过滤和排序
      * -- @note: [[TimeSeriesProc.repartition]]
      *
      * 周期检测: 对排好序的时间序列提取若干个检测周期
      * -- @note: [[TimeSeriesProc.periodDetect]]
      *
      * 数据预处理: 根据确定的周期对时间序列进行转换，确定时间序列的缺失率并进行插值补全
      * -- @note [[]]
      */
    object TimeSeriesProc extends Serializable {
      def transformDF(
                       input: DataFrame,
                       deviceIdCol: String,
                       timeCol: String,
                       unitNameCol: String,
                       unitIdCol: String,
                       gasConsistencyCol: String,
                       timeFormat: String = "yyyy-MM-dd HH:mm:ss",
                       timeIntervalFormat: String = "back", // minMax
                       intervalString: String = "1Min",
                       missRate: Double = 0.5
                     ): DataFrame = {

        val pattern = new Regex("([0-9]+)\\s*([a-zA-Z]+)")
        val interval = intervalString.trim match {
          case pattern(freqNum, unit) =>
            unit.toLowerCase() match {
              case "s" => freqNum.toInt * 1000L
              case "m" => freqNum.toInt * 60 * 1000L
              case "min" => freqNum.toInt * 60 * 1000L
              case "h" => freqNum.toInt * 60 * 60 * 1000L
              case "d" => freqNum.toInt * 24 * 60 * 60 * 1000L
            }
          case _ =>
            throw new Exception("您输入的时间段信息不能转为合法时间, 请输入整数 + 时间单位(d/h/min/s)")
        }

        // 将数据按设备ID进行分区并重新排序
        val rdd = repartition(input, deviceIdCol, timeCol, unitNameCol, unitIdCol, gasConsistencyCol, timeFormat,
          timeIntervalFormat, interval)
        rdd.flatMapValues {
          case (startTime, endTime, values) =>
            if (startTime >= endTime || values.isEmpty)
              Iterator.empty
            else {
              val period = periodDetect(values, 100, 400, 0.2)
              // 根据现场确认: 周期不会低于1s, 且一般4h内的数据单个序列最高20000条, 因此限定不能超过10000000
              // 以防止配置不高造成的栈溢出
              // 周期没识别好的情况
              if(period <= 1000L || (endTime - startTime) / period >= 10000000L || (endTime -startTime) / period <= 0)
                Iterator.empty
              else {
                try {
                  transformSeries(values, startTime, endTime, period, missRate)
                } catch {
                  case e: Exception => throw new Exception(s"插值过程出现异常${e.getMessage}")
                }
              }
            }
        }.values.map(values => (values.time, values.unitId, values.deviceId, values.gasConsistency,
          values.period, values.unitName)).toDF("time", "unitId", "deviceId", "gasConsistency", "period", "unitName")
      }


      /**
        * 重分区
        *
        * @param data              数据
        * @param deviceIdCol       设备ID
        * @param timeCol           时间
        * @param unitNameCol       煤矿名
        * @param unitIdCol         煤矿ID
        * @param gasConsistencyCol 瓦斯值
        * @param timeFormat        时间列的时间字符串格式
        * @return 分区后的数据 [设备ID， (起始时间, 截止时间, 序列)] @note: 注意每个分区中的数目不能超过executor内存限制, 否则会栈溢出
        */
      def repartition(
                       data: DataFrame,
                       deviceIdCol: String,
                       timeCol: String,
                       unitNameCol: String,
                       unitIdCol: String,
                       gasConsistencyCol: String,
                       timeFormat: String = "yyyy-MM-dd HH:mm:ss",
                       timeIntervalFormat: String = "back", // minMax
                       interval: Long = 4 * 60 * 60 * 1000L
                     ): RDD[(Int, (Long, Long, Array[GasValues]))] = {
        val timeFormatter = new SimpleDateFormat(timeFormat)
        data.select(deviceIdCol, timeCol, unitNameCol, unitIdCol, gasConsistencyCol).rdd.map {
          row =>
            var flag = true
            val deviceId = util.Try(row.getAs[Any](deviceIdCol).toString.toDouble.toInt).getOrElse {
              flag = false
              0
            }
            val time = util.Try(timeFormatter.parse(row.getAs[Any](timeCol).toString).getTime).getOrElse {
              flag = false
              0L
            }
            val unitName = util.Try(row.getAs[Any](unitNameCol).toString).getOrElse {
              flag = false
              null
            }
            val unitId = util.Try(row.getAs[Any](unitIdCol).toString.toDouble.toInt).getOrElse {
              flag = false
              0
            }
            val gasConsistency = util.Try(row.getAs[Any](gasConsistencyCol).toString.toDouble).getOrElse {
              flag = false
              0.0
            }
            (deviceId, (GasValues(deviceId, time, unitName, unitId, gasConsistency), flag))
        }.filter(_._2._2).map {
          case (deviceId, (values, _)) =>
            (deviceId, values)
        }.groupByKey().mapValues {
          iter =>
            if (timeIntervalFormat == "back") {
              // 当前时间按分钟取整
              val endTime = System.currentTimeMillis() / 60000L * 60000L
              val startTime = endTime - interval
              (startTime, endTime, iter.filter(value => endTime >= value.time && startTime <= value.time)
                .toArray.sortBy(_.time))
            } else {
              val res = iter.toArray.sortBy(_.time)
              (res.head.time, res.last.time, res)
            }
        }
      }

      /**
        * 周期发现
        *
        * @param series     输入的序列
        * @param num4level  确定周期水平的数据数目
        * @param num4circle 确定周期的数据数目
        * @param rate4noise 噪声比率
        * @return 周期
        */
      def periodDetect(series: Seq[GasValues],
                       num4level: Int,
                       num4circle: Int,
                       rate4noise: Double): Long = {
        val periods = periodCounter(series, num4level, num4circle, rate4noise).toArray
        if(periods.isEmpty)
          -1L
        else
          periods.maxBy(_._2)._1
      }


      def periodCounter(
                         series: Seq[GasValues],
                         num4level: Int,
                         num4circle: Int,
                         rate4noise: Double
                       ): mutable.Map[Long, Int] = {
        // 沟通后确认周期不会超过1s, 为了防止一些噪声导致统计次数过大, 限定只有超过该长度时才会认为是周期
        val minCircleValue = 1000L

        var sum = 0L
        var count = 0L
        var lastTime = 0L
        var index = 0

        require(num4level > 0 && num4circle > 0)

        // 周期的计数器
        var circleCounter = scala.collection.mutable.Map.empty[Long, Int]
        // 水平 todo: 水平的确认是基于均值的, 可能受异常值干扰, 建议后面改为中值
        var level = 0L
        while (index < series.length && index < num4circle) {
          val newTime = series(index).time
          val newCircle = newTime - lastTime
          if(newCircle > minCircleValue) {
            lastTime = newTime
            sum += (if (count > 0) newCircle else 0L)
            count += 1

            if (count > num4level || count == series.length) {
              // 首次确定level & 更新周期统计
              if (level == 0L) {
                level = sum / count
                // 先前因为level未知所有周期都进行了统计, 此时需要更新下统计的周期
                val output = scala.collection.mutable.Map.empty[Long, Int]
                circleCounter.foreach {
                  case (key, cnt) =>
                    updateCounter(circleCounter, key, rate4noise, level, output, cnt)
                }
                circleCounter = output
              } else {
                updateCounter(circleCounter, newCircle, rate4noise, level, circleCounter)
              }
            } else {
              // 非首次执行
              if (count > 1) {
                circleCounter += newCircle -> (circleCounter.getOrElse(newCircle, 0) + 1)
              }
            }
          }

          index += 1
        }

        circleCounter
      }

      /**
        * 更新周期统计器
        *
        * @param input      输入的周期统计器
        * @param newCircle  新的周期值
        * @param rate4noise 噪声比
        * @param level      水平
        * @param output     输出的周期统计器
        * @param addCnt     每次执行时向输出的周期统计器中对应元素追加的值
        * @return 无  效果是可变变量 --输出的周期统计器[output]发生对应变化
        */
      def updateCounter(
                         input: scala.collection.mutable.Map[Long, Int],
                         newCircle: Long,
                         rate4noise: Double,
                         level: Long,
                         output: scala.collection.mutable.Map[Long, Int],
                         addCnt: Int = 1
                       ): Unit = {
        //    println(input.toArray.sortBy(_._2 * -1).mkString(", "))
        val findBucketBelong2 = input.toArray.sortBy(_._2 * -1).exists {
          case (crl, _) =>
            if (abs(newCircle - crl) < level * rate4noise) {
              output += (crl -> (output.getOrElse(crl, 0) + addCnt))
              true
            }
            else
              false
        }

        if (!findBucketBelong2)
          output += (newCircle -> addCnt)
      }


      // @todo 会产生null值, 看下为什么
      def transformSeries(
                           series: Seq[GasValues],
                           startTime: Long,
                           endTime: Long,
                           period: Long,
                           maxMissRate: Double
                         ): Iterator[GasValues] = {
        // 算法:
        // 1）根据起始时间和时间间隔计算缺失率
        // 2）if 缺失率 > maxMissRate 丢掉 else 插值
        // ----
        // 插值算法:
        // 1）根据起始时间和时间间隔制造箱子用于存放时间序列
        // 2）每条记录找到离其最近的箱子 => 由于等距, 可以利用记录的时间和两个箱子的中点位置关系判定其归属于哪个箱子
        // 3）每个箱子如果多条记录取其平均值, 如果无记录利用线性插值填充
        val cnt = (endTime - startTime) / period
        val missRate = min(max(1 - series.length / (cnt + 1.0), 0.0), 1.0) // 只有几条数据时, 会有舍入误差, 不过此时数据过小没有意义
        if (missRate > maxMissRate || cnt <= 0 || series.isEmpty || period <= 0)
          Iterator.empty
        else {
          interpolate(series, startTime, endTime, period).iterator
        }
      }


      /**
        * 插值算法
        * @param series 进行插值的序列
        * @param startTime 起始时间
        * @param endTime 截止时间
        * @param period 周期
        * @return
        */
      def interpolate(series: Seq[GasValues],
                      startTime: Long,
                      endTime: Long,
                      period: Long
                     ): Array[GasValues] = {
        val cnt = ((endTime - startTime) / period).toInt
        // 构造一个存储器录入数据, 长度固定
        // 注意: 有录入动作时需要: 判定录入的index是否小于数组长度且大于等于0 & 更新最后录入记录的状态
        val res = mutable.ArrayBuilder.make[GasValues]()

        // 最后录入的记录器, 用于尾部缺失时的插值
        var cursor = -1L
        var lastValue: GasValues = null
        // 监视器, 监控目前录入的总条数, 双保险
        var lth = 0

        var index = 0
        while (index < series.length) {
          val value = series(index)
          index += 1
          // 该记录所归属的时间组id => 每条记录选择距离其最近的时间组 -- 注意利用了等间隔 + 中线
          val groupId = ((value.time - startTime - period / 2) / period).toInt
          val groupTime = groupId * period + startTime + period / 2
          val newValue = value.updateTime(groupTime).updatePeriod(period)

          if(groupId >= 0 && groupId < cnt && lth < cnt) {
            // 如果该次和上次记录归属于同一组录入[录入动作]
            require(groupId >= cursor, "算法异常: 新数据的id应该比cursor要高")
            if(groupId > cursor) {
              // 开头缺失 => 用最近的非缺失值填充
              if(cursor == -1L) {
                for(i <- 0 to groupId ) {
                  res += newValue.updateTime(startTime + period * i)
                  lth += 1
                }
              } else {
                // 中间缺失 => 用线性插值填充
                require(lastValue != null, "算法异常: 非开头序列, cursor记录值不能为null")
                val gap = (newValue.gasConsistency - lastValue.gasConsistency) / (groupId - cursor).toDouble

                val lastTime = lastValue.time
                val startId = cursor.toInt
                val lastGasValue = lastValue.gasConsistency
                // [录入动作]
                for (i <- (startId + 1) to groupId) {
                  val newGasValue = lastGasValue + (i - startId) * gap // updateValue

                  res += newValue
                    .updateTime(lastTime + (i - startId) * period)
                    .updateValue(newGasValue)

                  lth += 1
                }
              }

              cursor = groupId
              lastValue = newValue
            }
          }
        }

        if(cursor >= 0){
          // 双保险
          require(cursor == lth - 1 && lth <= cnt, "算法异常: 游标位置和计数不一致")

          // 尾部的插值 --用最近的非缺失值填充
          if (cursor < cnt - 1) {
            // [录入动作] 此时不用更新最后的记录器
            for (i <- (cursor.toInt + 1) until cnt) {
              res += lastValue
                .updateTime(lastValue.time + (i - cursor) * period)
                .updatePeriod(period)
              lth += 1
            }
          }

          res.result()
        } else
          Array.empty[GasValues]
      }


    }



    import TimeSeriesProc._
    val deviceIdCol: String = "deviceid"
    val timeCol: String = "valuetime"
    val unitNameCol: String = "unitname"
    val unitIdCol: String = "unitid"
    val gasConsistencyCol: String = "value"
    val timeFormat: String = "yyyy-MM-dd HH:mm:ss"
    val timeIntervalFormat: String = "minMax" // "back"
    val intervalString: String = "4h"
    val missRate: Double = 0.5

    val tableName = "ysk.t_fact_yx_sbyxjl_lsb_zl"
    val data = spark.sql(s"select $deviceIdCol, $timeCol, $unitNameCol, $unitIdCol, $gasConsistencyCol from $tableName").na.drop("any")

//    spark.sql(s"select $deviceIdCol, count(1) as cnt from $tableName group by $deviceIdCol order by cnt desc").show()
    data.cache()
    val res = transformDF(
          data,
          deviceIdCol,
          timeCol,
          unitNameCol,
          unitIdCol,
          gasConsistencyCol,
          timeFormat,
          timeIntervalFormat,
          intervalString,
          missRate
        )

    /*val input = data
    val pattern = new Regex("([0-9]+)\\s*([a-zA-Z]+)")
    val interval = intervalString.trim match {
      case pattern(freqNum, unit) =>
        unit.toLowerCase() match {
          case "s" => freqNum.toInt * 1000L
          case "min" => freqNum.toInt * 60 * 1000L
          case "h" => freqNum.toInt * 60 * 60 * 1000L
          case "d" => freqNum.toInt * 24 * 60 * 60 * 1000L
        }
      case _ =>
        throw new Exception("您输入的时间段信息不能转为合法时间, 请输入整数 + 时间单位(d/h/min/s)")
    }

    import spark.implicits._
    // 将数据按设备ID进行分区并重新排序


    val rdd = repartition(input, deviceIdCol, timeCol, unitNameCol, unitIdCol, gasConsistencyCol, timeFormat,
      timeIntervalFormat, interval)

    /*val resuu = rdd.flatMapValues {
      case (startTime, endTime, values) =>
        if (startTime >= endTime)
          throw new Exception("起始时间小于截止时间")

        if (values.isEmpty)
          throw new Exception("迭代器为空")

        if (startTime >= endTime || values.isEmpty)
          Iterator.empty
        else {
          val period = periodDetect(values, 20, 400, 0.2)
          Array(period).iterator
        }
    }
    println(resuu.count())
    resuu.take(100).foreach(println)*/

    val res = rdd.flatMapValues {
      case (startTime, endTime, values) =>
        if (startTime >= endTime)
          throw new Exception("起始时间小于截止时间")

        if (startTime >= endTime || values.isEmpty)
          Iterator.empty
        else {
          val period = periodDetect(values, 20, 400, 0.2)
          try {
            transformSeries(values, startTime, endTime, period, missRate)
          } catch {
            case e: Exception => throw new Exception(s"出现异常${e.getMessage}\r\n" +
              s"${values.mkString("\r\n")}")
          }
        }
    }.values.map(values => (values.time, values.unitId, values.deviceId, values.gasConsistency,
      values.unitName)).toDF("time", "unitId", "deviceId", "gasConsistency", "unitName")
*/
    res
  }


  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime()
    val mm = test()
    println(mm.count())
    mm.show()

    val t2 = System.nanoTime()
    println(t1 - t2)

  }

}
