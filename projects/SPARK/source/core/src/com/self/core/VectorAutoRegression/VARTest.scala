package com.self.core.VectorAutoRegression

import java.sql.Timestamp

import com.self.core.VectorAutoRegression.utils.ToolsForTimeSeriesWarp
import com.self.core.baseApp.myAPP
import com.self.core.featurePretreatment.utils.Tools
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, NullableFunctions, Row}
import org.apache.spark.{Partitioner, SparkException}

import scala.collection.mutable

object VARTest extends myAPP {
  def createData(): DataFrame = {
    val timeParser = new java.text.SimpleDateFormat("yyyy-MM")

    val seq = List(
      ("1", "2017-08", 199, 0, 197),
      ("1", "2017-01", 123, -150, -150),
      ("1", "2017-02", 9, -150, -150),
      ("1", "2017-02", 19, -10, -150),
      ("1", "2017-03", 19, -150, -150),
      ("1", "2017-07", 1, 50, -150),
      ("1", "2017-05", 199, -8, -150),
      ("1", "2017-07", 120, 7, 0),
      ("1", "2017-09", -200, 100, 50),
      ("1", "2017-10", 60, 50, 0),
      ("1", "2017-11", 199, 98, 0),
      ("1", "2017-12", 100, 0, 10),
      ("2", "2017-01", 99, -70, -150),
      //      ("2", null, 199, -150, -150),
      ("2", "2018-02", 1, -50, -15),
      ("2", "2018-05", 90, -0, -1),
      ("2", "2018-06", 19, -50, -15),
      //      ("2", null, 199, -150, -150),
      ("2", "2018-03", 1, -50, -15),
      ("2", "2018-07", 90, -0, -1),
      ("2", "2018-08", 19, -50, -15),
      ("2", "2018-09", 1, -50, -15),
      ("2", "2018-10", 90, -0, -1),
      ("2", "2018-11", 19, -50, -15)
    ).map(tup => (tup._1, new java.sql.Timestamp(timeParser.parse(tup._2).getTime), tup._3, tup._4, tup._5))

    sqlc.createDataFrame(seq).toDF("id", "dayTime", "x", "y", "z")
  }


  override def run(): Unit = {
    println("This is a main file.")


    /** 0)获得数据和一些初始信息 */
    val rawDataFrame: DataFrame = createData()
    val sQLContext = rawDataFrame.sqlContext
    val sparkContext = sQLContext.sparkContext

    val timeColName: String = "dayTime" // @todo 还需要一个将时间根据窗口和相位规则进行平滑和补全的工具函数

    // 数据只能为Numeric类型
    val variablesColNames = Array("x", "y", "z")
    // 列类型判断
    variablesColNames.foreach {
      name =>
        Tools.columnTypesIn(name, rawDataFrame, true, StringType, DoubleType, IntegerType, LongType, FloatType, ByteType)
    }

    val rawDataDF = rawDataFrame.na.drop(variablesColNames) // 只要有一个变量为空就会drop

    val timeSeriesFormat = "timeCol" // "timeIdCol"

    /** 1)获得时间序列窗口的id */
    val binningIdColName: String = Tools.nameANewCol("windowId", rawDataDF)

    val (dfAfterBinning: DataFrame, endWindowId: Long) = timeSeriesFormat match {
      case "timeCol" =>

        /** 时间序列起止时间和窗宽信息 */
        val (startTime: Long, endTime: Long) = {
          val timeParser = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          (timeParser.parse("2017-01-01 00:00:00").getTime,
            timeParser.parse("2019-01-01 00:00:00").getTime)
        }

        val windowUnit: String = "month"
        val windowLength: Int = try {
          "2".toDouble.toInt
        } catch {
          case e: Exception => throw new Exception(s"您输入的时间序列窗宽长度不能转为数值类型, 具体信息为${e.getMessage}")
        }
        require(windowLength > 0, "您输入的时间序列窗宽长度需要大于0")

        /** 确认start和end恰好是整数个窗宽 */
        val binningEndTime = ToolsForTimeSeriesWarp.binning(
          new Timestamp(endTime), startTime, windowLength, windowUnit)

        require(binningEndTime.getTime == endTime, s"您输入的'时间序列起止时间'和'时间序列窗宽'参数不符合逻辑：" +
          s"算子要求您输入的起止时间应该恰好是您输入的窗宽的整数倍：" +
          s"您输入的起始时间是${new Timestamp(startTime)}, 窗宽是${windowLength + windowUnit}, " +
          s"整数倍窗宽的截止时间应为$binningEndTime, 请您按需求调节起始和终止时间.")

        val endWindowId = ToolsForTimeSeriesWarp.binningForId(
          new Timestamp(endTime), startTime, windowLength, windowUnit) - 1 // 左闭右开区间，所以结束时间减掉1才是最大窗口id


        /** 将窗宽分箱获得 => 时间 + 箱子所在id */

        val binningUDF = NullableFunctions.udf(
          (time: Timestamp) => ToolsForTimeSeriesWarp.binningForId(time, startTime, windowLength, windowUnit)
        )

        val df = rawDataDF.withColumn(binningIdColName, binningUDF(col(timeColName)))
          .filter(col(binningIdColName).between(0, endWindowId))
        (df, endWindowId)

      case "timeIdCol" =>
        val schema = rawDataDF.schema.fields
        val endWindowId = try {
          rawDataDF.count() - 1
        } catch {
          case e: Exception => throw new Exception(s"算子在为无时间列的时间序列建立窗口时事变，可能的原因是数据为空。" +
            s"具体异常信息: ${e.getMessage}")
        }
        val df = sQLContext.createDataFrame(rawDataDF.rdd.zipWithIndex().map {
          case (row, windowId) =>
            Row.merge(Row(windowId), row)
        }, StructType(StructField(binningIdColName, LongType) +: schema))
        (df, endWindowId)
    }

    /** 2)每个窗口进行规约 --规约方式可以为最大、最小、均值、最早四种方式，如果一个窗口内某个变量全为null值则规约为Double.NaN值 */
    // 规约方式
    val reduction = "first" // "min", "mean", "first"
    import org.apache.spark.sql.functions.{first, max, mean, min}
    val DFAfterReduction: DataFrame = reduction match {
      case "max" =>
        dfAfterBinning.groupBy(col(binningIdColName))
          .agg(max(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
            variablesColNames.drop(1).map(name => max(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

      case "min" =>
        dfAfterBinning.groupBy(col(binningIdColName))
          .agg(min(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
            variablesColNames.drop(1).map(name => min(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

      case "mean" =>
        dfAfterBinning.groupBy(col(binningIdColName))
          .agg(mean(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
            variablesColNames.drop(1).map(name => mean(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

      case "first" =>
        dfAfterBinning.sort(col(timeColName)).groupBy(col(binningIdColName))
          .agg(first(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
            variablesColNames.drop(1).map(name => first(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)
      // 验证一下集群上是不是这样
    }


    /** 3)窗口补全和记录补全 */
    // 补全方式 "mean", "zero", "linear"
    def fillLinear(starRow: Seq[Double], gaps: Seq[Double], step: Int): Seq[Double] =
      starRow.zip(gaps).map { case (d1, gap) => d1 + gap * step }

    def windowComplement(DFAfterReduction: DataFrame, startWindowId: Long, endWindowId: Long, format: String): DataFrame = {
      /** 1)根据窗口id生成PairRDD并相机补全最大最小值 */
      var rdd: RDD[(Long, Seq[Double])] = DFAfterReduction.rdd.map {
        row =>
          (row.getAs[Long](binningIdColName), row.toSeq.drop(1).map(_.asInstanceOf[Double]))
      }

      val ((minWindowId, minWindowValue), (maxWindowId, maxWindowValue)) =
        try {
          format match {
            case "mean" =>
              val row = DFAfterReduction.select(variablesColNames.map(name => mean(col(name))): _*).head()
              ((rdd.keys.min(), row.toSeq.map(_.asInstanceOf[Double])),
                (rdd.keys.max(), row.toSeq.map(_.asInstanceOf[Double])))
            case "zero" =>
              val row = Row.fromSeq(variablesColNames.map(_ => 0.0))
              ((rdd.keys.min(), row.toSeq.map(_.asInstanceOf[Double])),
                (rdd.keys.max(), row.toSeq.map(_.asInstanceOf[Double])))
            case "linear" =>
              (rdd.min()(Ordering.by[(Long, Seq[Double]), Long](_._1)),
                rdd.max()(Ordering.by[(Long, Seq[Double]), Long](_._1)))
          }
        } catch {
          case e: Exception => throw new Exception("在时间规整阶段获取最大时间分箱id失败，可能的原因是：" +
            "1）数据有过多缺失值，在缺失值阶段数据为空，2）数据的时间列没有在时间序列起止时间内导致过滤后为空，" +
            s"3）其他原因。具体信息: ${e.getMessage}")
        }

      if (minWindowId != startWindowId) {
        rdd = rdd.union(sparkContext.parallelize(Seq((startWindowId, minWindowValue))))
      } // 如果开头缺失则找它最近时间的值补全

      if (maxWindowId != endWindowId) {
        rdd = rdd.union(sparkContext.parallelize(Seq((endWindowId, maxWindowValue))))
      } // 如果结尾缺失则找它最近时间的值补全

      /** 2)获得非空分区的数目 */
      /** 最后一个非空分区的id */
      val numParts = rdd.mapPartitions {
        iter =>
          if (iter.isEmpty) Iterator.empty else Iterator(1)
      }.count().toInt

      println(s"算子检测到原始分区数为${rdd.partitions.length}, 实际分区数为$numParts, 现已将空分区去除")

      /** 3)根据窗口id进行重分区 */
      class PartitionByWindowId(numParts: Int) extends Partitioner {
        override def numPartitions: Int = numParts

        override def getPartition(key: Any): Int =
          key match {
            case i: Long =>
              val maxNumEachPartition = scala.math.ceil(endWindowId / numParts.toDouble) // 最小为1.0
              (i / maxNumEachPartition).toInt // 必定大于等于0且小于numParts [[maxWindowId]]为最大值
            case _: Exception =>
              throw new SparkException("key的类型不是long")
          }
      }
      val rddPartitionByWinId: RDD[(Long, Seq[Double])] = rdd.coalesce(numParts).repartitionAndSortWithinPartitions(
        new PartitionByWindowId(numParts)
      ) // 确保分区后的顺序

      /** 4)定义分区 */
      class OverLapPartitioner(numParts: Int) extends Partitioner {
        override def numPartitions: Int = numParts

        override def getPartition(key: Any): Int = {
          val id = key match {
            case i: Long =>
              i >> 33
            case _: Exception =>
              throw new SparkException("key的类型不是long")
          }
          val modNum = (id % numParts).toInt
          if (modNum < 0) modNum + numParts else modNum
        }
      }

      val rePartitionRdd: RDD[(Long, (Long, Seq[Double]))] = rddPartitionByWinId.mapPartitionsWithIndex {
        (partitionId, iter) =>
          val partitionNum = partitionId.toLong << 33
          var i = 0
          val result = mutable.ArrayBuilder.make[(Long, (Long, Seq[Double]))]()
          iter.foreach {
            value =>
              i += 1
              if (partitionId != 0 && i == 1) {
                result += Tuple2(partitionNum - 1, value)
                result += Tuple2(partitionNum, value)
              } else {
                result += Tuple2(partitionNum + i - 1, value)
              }
          }
          result.result().toIterator
      }.partitionBy(new OverLapPartitioner(numParts))

      val maxPartitionIndex = rePartitionRdd.mapPartitionsWithIndex {
        case (index, iter) =>
          if (iter.isEmpty) Iterator.empty else Iterator(index)
      }.max()

      /** 按起止id补全 */
      val resultRdd = rePartitionRdd.mapPartitionsWithIndex(
        (index, iters) => {
          val result = mutable.ArrayBuilder.make[(Long, (Long, Seq[Double]))]()

          val iter = iters.toArray.sortBy(_._2._1).toIterator

          var lastWindowId = 0L
          var plus = 0L // 循环计数器
          var lastRow: Seq[Double] = null
          var i = 0
          var lastKey = 0L
          while (iter.hasNext) {
            val (key, (windowId, row)) = iter.next()

            if (i == 0) {
              result += Tuple2(key + plus, (windowId, row))
              lastWindowId = windowId
              lastKey = key
              lastRow = row
              plus += 1
            } else {
              val gap = row.zip(lastRow).map(tup => (tup._1 - tup._2) / (windowId - lastWindowId)) // 等差数列线性填充
              for (step <- 1 to (windowId - lastWindowId).toInt) {
                val fillValue = if (format == "linear") fillLinear(lastRow, gap, step) else minWindowValue
                result += Tuple2(lastKey + plus,
                  (lastWindowId + step, if (step == (windowId - lastWindowId)) row else fillValue))
                plus += 1
              }

              lastWindowId = windowId
              lastRow = row
            }

            i += 1
          }

          if (index == maxPartitionIndex)
            result.result().toIterator
          else
            result.result().dropRight(1).toIterator
        }
      )

      sQLContext.createDataFrame(resultRdd.values.map { case (windowId, values) => Row.fromSeq(windowId +: values) },
        StructType(StructField(binningIdColName, LongType) +: variablesColNames.map(name => StructField(name, DoubleType)))
      )
    }


    /** 4)结果输出 */
    val newDataFrame = windowComplement(DFAfterReduction, 0L, endWindowId, "linear")

    newDataFrame.show()



  }


}
