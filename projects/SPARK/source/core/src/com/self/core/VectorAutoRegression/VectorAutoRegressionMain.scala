package com.self.core.VectorAutoRegression

import java.sql.Timestamp

import breeze.linalg.{DenseMatrix, DenseVector}
import com.google.gson.JsonParser
import com.self.core.VectorAutoRegression.utils.ToolsForTimeSeriesWarp
import com.self.core.baseApp.myAPP
import com.self.core.featurePretreatment.utils.Tools
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, NullableFunctions, Row}

import scala.collection.mutable


object VectorAutoRegressionMain extends myAPP {
  def createData(): Unit = {
    val timeParser = new java.text.SimpleDateFormat("yyyy-MM")

    val seq = List(
      ("1", "2014-08", 199, 0, 197),
      ("1", "2014-01", 123, -150, -150),
      ("1", "2014-02", 9, -150, -150),
      ("1", "2014-02", 19, -10, -150),
      ("1", "2014-03", 19, -150, -150),
      ("1", "2014-07", 1, 50, -150),
      ("1", "2014-05", 199, -8, -150),
      ("1", "2014-07", 120, 7, 0),
      ("1", "2014-09", -200, 100, 50),
      ("2", "2014-01", 99, -70, -150),
      ("2", "2015-02", 1, -50, -15),
      ("2", "2015-07", 90, -0, -1),
      ("2", "2015-08", 19, -50, -15),
      ("2", "2015-09", 1, -50, -15),
      ("2", "2015-10", 90, -0, -1),
      ("2", "2015-11", 19, -50, -15),
      ("1", "2016-08", 199, 0, 197),
      ("1", "2016-01", 123, -150, -150),
      ("1", "2016-02", 9, -150, -150),
      ("1", "2016-02", 19, -10, -150),
      ("1", "2016-03", 19, -150, -150),
      ("1", "2016-07", 1, 50, -150),
      ("1", "2016-05", 199, -8, -150),
      ("1", "2016-07", 120, 7, 0),
      ("1", "2016-09", -200, 100, 50),
      ("2", "2017-01", 99, -70, -150),
      ("2", "2017-02", 1, -50, -15),
      ("2", "2017-07", 90, -0, -1),
      ("2", "2017-08", 19, -50, -15),
      ("2", "2017-09", 1, -50, -15),
      ("2", "2017-10", 90, -0, -1),
      ("2", "2017-11", 19, -50, -15),
      ("1", "2018-08", 199, 0, 197),
      ("1", "2018-01", 123, -150, -150),
      ("1", "2018-02", 9, -150, -150),
      ("1", "2018-02", 19, -10, -150),
      ("1", "2018-03", 19, -150, -150),
      ("1", "2018-07", 1, 50, -150),
      ("1", "2018-05", 199, -8, -150),
      ("1", "2018-07", 120, 7, 0),
      ("1", "2018-09", -200, 100, 50),
      ("2", "2019-01", 99, -70, -150),
      ("2", "2019-02", 1, -50, -15),
      ("2", "2019-07", 90, -0, -1),
      ("2", "2019-08", 19, -50, -15),
      ("2", "2019-09", 1, -50, -15),
      ("2", "2019-10", 90, -0, -1),
      ("2", "2019-11", 19, -50, -15),
      ("1", "2020-08", 199, 0, 197),
      ("1", "2020-01", 123, -150, -150),
      ("1", "2020-02", 9, -150, -150),
      ("1", "2020-02", 19, -10, -150),
      ("1", "2020-03", 19, -150, -150),
      ("1", "2020-07", 1, 50, -150),
      ("1", "2020-05", 199, -8, -150),
      ("1", "2020-07", 120, 7, 0),
      ("1", "2020-09", -200, 100, 50),
      ("2", "2021-01", 99, -70, -150),
      ("2", "2021-02", 1, -50, -15),
      ("2", "2021-07", 90, -0, -1),
      ("2", "2021-08", 19, -50, -15),
      ("2", "2021-09", 1, -50, -15),
      ("2", "2021-10", 90, -0, -1),
      ("2", "2021-11", 19, -50, -15)
    ).map(tup => (tup._1, new java.sql.Timestamp(timeParser.parse(tup._2).getTime), tup._3, tup._4, tup._5))

    val newDataFrame = sqlc.createDataFrame(seq).toDF("id", "dayTime", "x", "y", "z")

    val rddTableName = "模拟时间序列_XFdCFEE5"
    newDataFrame.registerTempTable(rddTableName)
    newDataFrame.sqlContext.cacheTable(rddTableName)
    outputrdd.put(rddTableName, newDataFrame)
  }


  def testTimeSeriesWarping(): Unit = {
    createData()

    /** 0)获得数据和一些初始信息 */
    val jsonparam =
      """{"RERUNNING":{"nodeName":"时间序列规整_1","preNodes":[{"checked":true,"id":"模拟时间序列_XFdCFEE5"}],"rerun":"false"},"inputTableName":"模拟时间序列_XFdCFEE5","timeSeriesFormat":{"reduction":"first","startEndTime":["2014-01-01 00:00:00","2022-01-01 00:00:00"],"timeColName":[{"datatype":"timestamp","index":1,"name":"dayTime"}],"timeWindowIdCol":"windowIdCol","value":"timeCol","windowLength":"1","windowUnit":"month"},"variablesColNames":[{"datatype":"int","index":2,"name":"x"},{"datatype":"int","index":3,"name":"y"},{"datatype":"int","index":4,"name":"z"}]}"""
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
    val z1 = outputrdd
    val rddTableName = "时间序列规整_1_pKdufqmW"

    val tableName = pJsonParser.get("inputTableName").getAsString

    val rawDataFrame: DataFrame = try {
      outputrdd.get(tableName).asInstanceOf[DataFrame]
    } catch {
      case e: Exception => throw new Exception(s"获取数据表失败，具体信息${e.getMessage}")
    }

    val sQLContext = rawDataFrame.sqlContext
    val sparkContext = sQLContext.sparkContext

    // 数据只能为Numeric类型
    val variablesColObj = pJsonParser.get("variablesColNames").getAsJsonArray

    val variablesColNames = Array.range(0, variablesColObj.size()).map {
      i =>
        val name = variablesColObj.get(i).getAsJsonObject.get("name").getAsString
        Tools.columnTypesIn(name, rawDataFrame, true, StringType, DoubleType, IntegerType, LongType, FloatType, ByteType)
        name
    }

    val rawDataDF = rawDataFrame.na.drop(variablesColNames) // 只要有一个变量为空就会drop

    val timeSeriesFormatObj = pJsonParser.get("timeSeriesFormat").getAsJsonObject // "timeCol" // "timeIdCol"


    /** 1)获得时间序列窗口的id */
    val binningIdColName = timeSeriesFormatObj.get("timeWindowIdCol").getAsString
    Tools.validNewColName(binningIdColName, rawDataDF)

    val (dFAfterReduction: DataFrame, endWindowId: Long) = timeSeriesFormatObj.get("value").getAsString match {
      case "timeCol" =>

        /** 时间序列起止时间和窗宽信息 */
        val timeColName = timeSeriesFormatObj.get("timeColName")
          .getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString

        val (startTime: Long, endTime: Long) = try {
          val interval = timeSeriesFormatObj.get("startEndTime").getAsJsonArray
          val timeParser = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          (timeParser.parse(interval.get(0).getAsString).getTime,
            timeParser.parse(interval.get(1).getAsString).getTime)
        } catch {
          case e: Exception => throw new Exception(s"在获得时间序列起止时间过程中异常。具体信息${e.getMessage}")
        }

        val windowUnit: String = try {
          timeSeriesFormatObj.get("windowUnit").getAsString
        } catch {
          case e: Exception => throw new Exception(s"窗宽单位获取过程中失败，具体信息${e.getMessage}")
        }
        val windowLength: Int = try {
          timeSeriesFormatObj.get("windowLength").getAsString.toDouble.toInt
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

        /** 2)每个窗口进行规约 --规约方式可以为最大、最小、均值、最早四种方式，如果一个窗口内某个变量全为null值则规约为Double.NaN值 */

        val reduction = timeSeriesFormatObj.get("reduction").getAsString // "min", "mean", "first"
      val dFAfterReduction: DataFrame = reduction match {
        case "max" =>
          df.groupBy(col(binningIdColName))
            .agg(max(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
              variablesColNames.drop(1).map(name => max(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

        case "min" =>
          df.groupBy(col(binningIdColName))
            .agg(min(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
              variablesColNames.drop(1).map(name => min(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

        case "mean" =>
          df.groupBy(col(binningIdColName))
            .agg(mean(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
              variablesColNames.drop(1).map(name => mean(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

        case "first" =>
          df.sort(col(timeColName)).groupBy(col(binningIdColName))
            .agg(first(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
              variablesColNames.drop(1).map(name => first(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)
        // 验证一下集群上是不是这样
      }

        (dFAfterReduction, endWindowId)

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
        }, StructType(StructField(binningIdColName, LongType) +: schema)) // @todo: 需要将变量的类型转为double

        (df, endWindowId)
    }


    /** 3)窗口补全和记录补全 */
    val format = "linear"
    var rddAfterReduction = dFAfterReduction.rdd.map {
      row =>
        val windowId = row.getAs[Long](binningIdColName)
        (windowId, variablesColNames.map(name => row.getAs[Double](name)))
    }.sortByKey(ascending = true).zipWithIndex().map {
      case ((windowId, features), index) =>
        (index, (windowId, features))
    }

    val (_, (minWindowId, minWindowValue)) = rddAfterReduction
      .min()(Ordering.by[(Long, (Long, Array[Double])), Long](_._2._1))
    val (maxIndex, (maxWindowId, maxWindowValue)) = rddAfterReduction
      .max()(Ordering.by[(Long, (Long, Array[Double])), Long](_._2._1))

    val (fillValues4min, fillValues4max) = try {
      format match {
        case "mean" =>
          val res = dFAfterReduction.select(variablesColNames.map(name => mean(col(name))): _*)
            .head().toSeq.map(_.asInstanceOf[Double])
          (res.toArray, res.toArray)

        case "zero" =>
          (variablesColNames.map(_ => 0.0), variablesColNames.map(_ => 0.0))

        case "linear" =>
          (minWindowValue, maxWindowValue)
      }
    } catch {
      case e: Exception => throw new Exception("在时间规整阶段获取最大时间分箱id失败，可能的原因是：" +
        "1）数据有过多缺失值，在缺失值阶段数据为空，2）数据的时间列没有在时间序列起止时间内导致过滤后为空，" +
        s"3）其他原因。具体信息: ${e.getMessage}")
    }

    if (minWindowId != 0L) {
      rddAfterReduction = rddAfterReduction.union(sparkContext.parallelize(Seq((0L, (0L, fillValues4min)))))
    } // 如果开头缺失则找它最近时间的值补全

    if (maxWindowId != endWindowId) {
      rddAfterReduction = rddAfterReduction.union(sparkContext.parallelize(Seq((maxIndex + 1, (endWindowId, fillValues4max)))))
    } // 如果结尾缺失则找它最近时间的值补全

    val rddLag = rddAfterReduction.map {
      case (index, (windowId, features)) =>
        (index - 1, (windowId, features))
    }

    // 补全方式 "mean", "zero", "linear"
    def fillLinear(starRow: Array[Double], endRow: Array[Double], step: Int, gaps: Int): Array[Double] = {
      endRow.zip(starRow).map { case (start, end) => start + (end - start) * step / gaps }
    }

    val res = rddAfterReduction.join(rddLag).flatMapValues {
      case ((windowId1, features1), (windowId2, features2)) =>
        val gaps = windowId2 - windowId1
        format match {
          case "mean" =>
            (windowId1.toInt, features1) +: Array.range(
              windowId1.toInt + 1, windowId2.toInt).map(index => (index, fillValues4min))

          case "zero" =>
            (windowId1.toInt, features1) +: Array.range(
              windowId1.toInt + 1, windowId2.toInt).map(index => (index, fillValues4min))

          case "linear" =>
            Array.range(windowId1.toInt, windowId2.toInt).map(
              step => (step, fillLinear(features1, features2, step - windowId1.toInt, gaps.toInt))) // @todo: 改成while循环创建
        }
    }.values


    /** 4)结果输出 */

    val newDataFrame = sQLContext.createDataFrame(
      res.map {
        case (windowId, values) => Row.fromSeq(windowId.toLong +: values)
      },
      StructType(
        StructField(binningIdColName, LongType) +: variablesColNames.map(name => StructField(name, DoubleType))
      )
    )

    newDataFrame.show()
    newDataFrame.registerTempTable(rddTableName)
    newDataFrame.sqlContext.cacheTable(rddTableName)
    outputrdd.put(rddTableName, newDataFrame)
  }

  def testVAR(): Unit = {
    testTimeSeriesWarping()

    println("This is a test file.")
    /** 0)获得数据和一些初始信息 */
    val jsonparam =
      """{"RERUNNING":{"nodeName":"时间序列规整_1","preNodes":[{"checked":true,"id":"模拟时间序列_XFdCFEE5"}],"rerun":"false"},"inputTableName":"时间序列规整_1_pKdufqmW","timeSeriesFormat":{"reduction":"first","startEndTime":["2017-01-01 00:00:00","2019-01-01 00:00:00"],"timeColName":[{"datatype":"timestamp","index":1,"name":"dayTime"}],"timeWindowIdCol":"windowIdCol","value":"timeCol","windowLength":"2","windowUnit":"month"},"variablesColNames":[{"datatype":"int","index":2,"name":"x"},{"datatype":"int","index":3,"name":"y"},{"datatype":"int","index":4,"name":"z"}]}"""
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
    val z1 = outputrdd
    val rddTableName = "时间序列规整_1_pKdufqmW"

    val tableName = pJsonParser.get("inputTableName").getAsString

    val rawDataFrame: DataFrame = try {
      outputrdd.get(tableName).asInstanceOf[DataFrame]
    } catch {
      case e: Exception => throw new Exception(s"获取数据表失败，具体信息${e.getMessage}")
    }

    val sQLContext = rawDataFrame.sqlContext
    val sparkContext = sQLContext.sparkContext

    val windowIdColName = "windowIdCol"

    rawDataFrame.orderBy(col(windowIdColName)).show()

    Tools.columnTypesIn(windowIdColName, rawDataFrame, true, LongType, IntegerType)

    val variablesColObj = pJsonParser.get("variablesColNames").getAsJsonArray
    val variablesColNames = Array.range(0, variablesColObj.size()).map {
      i =>
        val name = variablesColObj.get(i).getAsJsonObject.get("name").getAsString
        Tools.columnTypesIn(name, rawDataFrame, true, DoubleType)
        name
    }


    /** VAR模型的信息 */
    val k = variablesColNames.length // k维
    val p = 3 // 阶
    val timeT = rawDataFrame.count()

    println("有多少个序列", timeT)

    require(timeT < (1.toLong << 32), "目前算法要求时间序列数目不大于2的32次方")
    val requireCount = k * k * p + (k + 1) * k / 2
    require(timeT > requireCount.toLong, s"根据您输入的滞后阶数和变量列数，模型总共需要估计${requireCount}个参数，" +
      s"时间序列序列数需要超过该数才满足过度识别条件, 您输入的序列数有${timeT}个, " +
      s"您可以增加序列数或者减少‘滞后阶数’或‘变量数’") // k*k*p + (k + 1)*k/2


    val rdd = rawDataFrame.rdd.map {
      row =>
        val windowId = row.getAs[Long](windowIdColName)
        val features = variablesColNames.map(name => row.getAs[Double](name))
        (windowId, features)
    }

    def findMiddleTwo(width: Long, length: Long)(z: Long): Seq[Long] = {
      Seq(0L, z, z - width, length).sorted.slice(1, 3) // 取中间两个
    }

    case class Meta(lag: Int, value: Array[Double])

    println("增加之前:")
    println(rdd.keys.collect().sorted.mkString(","))

    val changeRdd: RDD[(Long, (Int, Array[Double]))] = rdd.flatMap {
      row =>
        val windowId = row._1
        val Seq(start, end) = findMiddleTwo(p, timeT - 1 - p)(windowId)

        val res = mutable.ArrayBuilder.make[(Long, (Int, Array[Double]))]()
        var dst_window = end
        while (dst_window >= start) {
          res += Tuple2(dst_window, Tuple2((windowId - dst_window).toInt, row._2)) // (发往窗口id, (滞后阶数, 对应值))
          dst_window -= 1
        }
        res.result()
    }
    println("补全之后")
    changeRdd.collect().sortBy(_._1).foreach(println)

    val zeroValue: mutable.Set[(Int, Array[Double])] = scala.collection.mutable.Set.empty
    val seqOp = (map: mutable.Set[(Int, Array[Double])], value: (Int, Array[Double])) => {
      map += value
      map
    }

    val fillSeriesRdd: RDD[(Long, Map[Int, Array[Double]])] = changeRdd.aggregateByKey(zeroValue)(seqOp, (m1, m2) => m1 ++ m2).mapValues {
      set =>
        set.toMap
    }


    // y0, y1, ..., yk-1     0, 1, ..., p => 其中每一行的逻辑如下
    // y0 * lag(0, y0), y1 * lag(0, y0), y2 * lag(0, y0), ..., yk-1 * lag(0, y0),
    // y0 * lag(0, y1), y1 * lag(0, y1), y2 * lag(0, y1), ..., yk-1 * lag(0, y1),
    // ...  ...  ...
    // y0 * lag(0, yk-1), y1 * lag(0, yk-1), y2 * lag(0, yk-1), ..., yk-1 * lag(0, yk-1),
    // y0 * lag(1, y0), y1 * lag(1, y0), y2 * lag(1, y0), ..., yk-1 * lag(1, y0),
    // y0 * lag(1, y1), y1 * lag(1, y1), y2 * lag(1, y1), ..., yk-1 * lag(1, y1),
    // ...  ...  ...
    // y0 * lag(1, yk-1), y1 * lag(1, yk-1), y2 * lag(1, yk-1), ..., yk-1 * lag(1, yk-1),
    // ...  ...  ...
    // ...  ...  ...
    // y0 * lag(p, y0), y1 * lag(p, y0), y2 * lag(p, y0), ..., yk-1 * lag(p, y0),
    // y0 * lag(p, y1), y1 * lag(p, y1), y2 * lag(p, y1), ..., yk-1 * lag(p, y1),
    // ...  ...  ...
    // y0 * lag(p, yk-1), y1 * lag(p, yk-1), y2 * lag(p, yk-1), ..., yk-1 * lag(p, yk-1),
    val rddForCorr: RDD[(Long, Array[Double])] = fillSeriesRdd.mapValues {
      mp =>
        val values = mp(0)
        Array.range(0, p + 1).flatMap(
          i =>
            mp(i).flatMap {
              v => values.map(_ * v)
            } // @todo: 验证一下
        )
    }


    val corr = rddForCorr.values.reduce {
      case (corr1, corr2) => corr1.zip(corr2).map {
        case (d1, d2) => d1 + d2
      }
    }.map(_ / (timeT - p - 1)) // 求自相关系数

    println("最终的相关系数矩阵")
    corr.foreach(println)

    val numFeatures = variablesColNames.length
    require(corr.length == (p + 1) * numFeatures * numFeatures,
      "最终的求出系数的长度不能够成相关系数矩阵")


    val rho = Array.range(0, p + 1).map {
      lag =>
        val corrValue = corr.slice(lag * numFeatures * numFeatures, (lag + 1) * numFeatures * numFeatures)
        (lag, new DenseMatrix[Double](numFeatures, numFeatures, corrValue))
    }.toMap

    rho.foreach(println)

    /** 形成一个大的矩阵 */
    println("----")
    val lxx = DenseMatrix.zeros[Double](numFeatures * p, numFeatures * p)

    Array.range(0, p).foreach {
      horizonId =>
        Array.range(0, p).foreach {
          verticalId =>
            val index = if(verticalId <= horizonId) horizonId - verticalId else verticalId - horizonId
            lxx(verticalId * numFeatures until (verticalId + 1)*numFeatures,
              horizonId * numFeatures until (horizonId + 1)*numFeatures) := rho(index)
        }
    }
    println(lxx)

    println("----")
    var lxy = DenseMatrix.zeros[Double](0, numFeatures)
    Array.range(1, p + 1).foreach {
      index =>
        lxy = DenseMatrix.vertcat(lxy, rho(index))
    }
    println(lxy)

    import breeze.linalg.inv

    val coefficients: DenseMatrix[Double] = try{
      inv(lxx) * lxy
    } catch {
      case e: Exception => throw new Exception("在通过Yule-Walker方程估计向量自回归系数时出现异常，可能的原因是" +
        s"输入的若干变量之间相关性非常大（例如两组变量值完全或接近完全一致），具体信息:${e.getMessage}")
    }

    println("最终系数")
    println(coefficients)
    val coefResult = mutable.Map.empty[Int, DenseMatrix[Double]]
    for(i <- 0 until p) {
      coefResult += (i -> coefficients(i * numFeatures until (i + 1)*numFeatures, ::))
    }

    println("放到map中")
    coefResult.keys.foreach(println)


    val (lastWindowId: Long, lastMap: Map[Int, Array[Double]]) = fillSeriesRdd.filter(_._1 == timeT - 1 - p).first()

    println("lastMap:", lastMap)
    val predictSteps = 10
    var i = 0
    val res = mutable.ArrayBuilder.make[(Long, Map[Int, Array[Double]])]()
    var mp = lastMap
    while (i < predictSteps) {
      mp = mp - p
      mp = mp.map {
        case (key, value) => (key + 1, value)
      }

      println("mp的值")
      mp.foreach {
        case (key, value) => println(key, value)
      }

      var predictValue = new Array[Double](numFeatures)
      for (step <- 0 until p) {
        println(s"第$step, 次循环")
        val mt1 = coefResult(step)
        println("mt1:", mt1.rows, mt1.cols)
        val v1 = new DenseVector[Double](mp(step + 1))
        println("v1:", v1.length, v1)

        val res: DenseVector[Double] = coefResult(step) * new DenseVector[Double](mp(step + 1))
        println("res", res)
        predictValue = res.data.zip(predictValue).map {
          case (d1, d2) => d1 + d2
        }
        println("predictValue", predictValue.mkString(","))
      }

      mp += (0 -> predictValue)
      res += (lastWindowId + 1 + i -> mp)

      i += 1
    }

    println("最终结果")


    val rdd4DF: RDD[Row] = fillSeriesRdd.union(sparkContext.makeRDD(res.result(), 1)).map {
      case (windowId, mAp) => Row.fromSeq(windowId +: mAp(0))
    }

    val newDataFrame = sQLContext.createDataFrame(rdd4DF, StructType(
      StructField(windowIdColName, LongType) +: variablesColNames.map(name => StructField(name, DoubleType))
    )).orderBy(col(windowIdColName))

    newDataFrame.show(200)


  }


  override def run(): Unit = {
//        val values = Array("y1", "y2", "y3")
//
//        val res  = Array.range(0, 5 + 1).flatMap(i => values.flatMap(dim1 => values.map(name => name + " * " + s"lag($dim1, $i)"))).mkString(", ")
//        println(res)

//    testTimeSeriesWarping()

        testVAR()

//    println(DenseMatrix.tabulate(3, 3){case (i, j) => i.toDouble + j} * new DenseVector[Double](Array(1.0, 2, 3)))


  }
}
