package com.self.core.trailCorner

import com.self.core.baseApp.myAPP
import com.self.core.trailCorner.models.{Point, PointDiffStat}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 轨迹盘旋识别
  * ----
  * 盘旋：较短时间内轨迹的转角过大
  * 说明:
  *
  * @note 构造了一个实验轨迹数据, 该轨迹中包含一个10分钟内转角大于90°的转弯，希望通过算法将其识别出来；
  *       另外设定转弯角度和时间的阈值之类的参数, 可以自由伸缩调节识别的不同角速度的转弯
  */
object hoverCorner extends myAPP {

  /**
    * 试验的轨迹数据
    * ----
    * 轨迹字段含义: 轨迹点元数据[ 时间戳(秒), 纬度, 经度 ]
    */
  val trailData: Array[Point] =
    Array(
      Point(1479120777000L, 15.174017, 98.0589),
      Point(1479120897000L, 15.048867, 98.306783),
      Point(1479121017000L, 14.9263, 98.552933),
      Point(1479121138000L, 14.80305, 98.799783),
      Point(1479121258000L, 14.678933, 99.046983),
      Point(1479121378000L, 14.552767, 99.2976),
      Point(1479121499000L, 14.43415, 99.5551),
      Point(1479121619000L, 14.41715, 99.820833),
      Point(1479121740000L, 14.400683, 100.075583),
      Point(1479121860000L, 14.34385, 100.301483),
      Point(1479121980000L, 14.294067, 100.500267),
      Point(1479122100000L, 14.247217, 100.686683),
      Point(1479122220000L, 14.12705, 100.76635),
      Point(1479122341000L, 13.963117, 100.723433),
      Point(1479122461000L, 13.809983, 100.683267),
      Point(1479122581000L, 13.668017, 100.645483),
      Point(1479122701000L, 13.53585, 100.610817),
      Point(1479122822000L, 13.439033, 100.63725),
      Point(1479122942000L, 13.519017, 100.71175),
      Point(1479123062000L, 13.611383, 100.7399),
      Point(1479134791000L, 14.112633, 98.550533),
      Point(1479134802000L, 14.129967, 98.2745),
      Point(1479134923000L, 14.1861, 98.005683),
      Point(1479135043000L, 14.252533, 97.739267),
      Point(1479135284000L, 14.387967, 97.193033),
      Point(1479135524000L, 14.5205, 96.6523),
      Point(1479135644000L, 14.585383, 96.38555),
      Point(1479135765000L, 14.650617, 96.116383),
      Point(1479135885000L, 14.71465, 95.850983),
      Point(1479136005000L, 14.779017, 95.582167),
      Point(1479136126000L, 14.843733, 95.3106),
      Point(1479136246000L, 14.907083, 95.0428),
      Point(1479136366000L, 14.97025, 94.775017),
      Point(1479136486000L, 15.033767, 94.50345),
      Point(1479136607000L, 15.0966, 94.234283),
      Point(1479136727000L, 15.15805, 93.96855),
      Point(1479136847000L, 15.220017, 93.699383),
      Point(1479136968000L, 15.2801, 93.437083),
      Point(1479137088000L, 15.341717, 93.166883),
      Point(1479137208000L, 15.401983, 92.900133),
      Point(1479137329000L, 15.46275, 92.629933),
      Point(1479137449000L, 15.529517, 92.360767),
      Point(1479137569000L, 15.5944, 92.09365),
      Point(1479137689000L, 15.625817, 91.825517),
      Point(1479137810000L, 15.6902, 91.555667),
      Point(1479137930000L, 15.753717, 91.2889),
      Point(1479138050000L, 15.8174, 91.020433),
      Point(1479138171000L, 15.881083, 90.7499),
      Point(1479138291000L, 15.943567, 90.482783),
      Point(1479138411000L, 16.005883, 90.215),
      Point(1479138532000L, 16.071117, 89.947883),
      Point(1479138652000L, 16.136, 89.6825),
      Point(1479138773000L, 16.19935, 89.421917),
      Point(1479138892000L, 16.263883, 89.1555),
      Point(1479139013000L, 16.3281, 88.889083),
      Point(1479139133000L, 16.390567, 88.627467)
    )

  val trailData2 =
    Array(
      ("2016/11/13 15:28:40", "7982S", 45.393483, 134.182317),
      ("2016/11/13 15:30:41", "7982S", 45.6484, 134.323433),
      ("2016/11/13 15:32:41", "7982S", 45.892333, 134.459733),
      ("2016/11/13 15:36:41", "7982S", 46.395817, 134.745367),
      ("2016/11/13 15:38:41", "7982S", 46.649533, 134.891633),
      ("2016/11/13 15:40:41", "7982S", 46.902383, 135.003717),
      ("2016/11/13 15:42:41", "7982S", 47.154733, 135.031183),
      ("2016/11/13 15:48:42", "7982S", 47.9272, 135.1299),
      ("2016/11/13 16:14:45", "7982S", 50.660233, 132.811783),
      ("2016/11/13 15:54:44", "7982S", 48.676167, 135.021067),
      ("2016/11/13 16:16:46", "7982S", 50.861417, 132.588967),
      ("2016/11/13 16:18:46", "7982S", 51.062767, 132.363567)
    )

  /** 轨迹数据的标签 --主要是时间列，方便绘图时观察用的 */
  val trailDataLabels: Map[Int, String] =
    Map(
      (0, "2016-11-14 18:52:57"),
      (1, "2016-11-14 18:54:57"),
      (2, "2016-11-14 18:56:57"),
      (3, "2016-11-14 18:58:58"),
      (4, "2016-11-14 19:00:58"),
      (5, "2016-11-14 19:02:58"),
      (6, "2016-11-14 19:04:59"),
      (7, "2016-11-14 19:06:59"),
      (8, "2016-11-14 19:09:00"),
      (9, "2016-11-14 19:11:00"),
      (10, "2016-11-14 19:13:00"),
      (11, "2016-11-14 19:15:00"),
      (12, "2016-11-14 19:17:00"),
      (13, "2016-11-14 19:19:01"),
      (14, "2016-11-14 19:21:01"),
      (15, "2016-11-14 19:23:01"),
      (16, "2016-11-14 19:25:01"),
      (17, "2016-11-14 19:27:02"),
      (18, "2016-11-14 19:29:02"),
      (19, "2016-11-14 19:31:02"),
      (20, "2016-11-14 22:46:31"),
      (21, "2016-11-14 22:46:42"),
      (22, "2016-11-14 22:48:43"),
      (23, "2016-11-14 22:50:43"),
      (24, "2016-11-14 22:54:44"),
      (25, "2016-11-14 22:58:44"),
      (26, "2016-11-14 23:00:44"),
      (27, "2016-11-14 23:02:45"),
      (28, "2016-11-14 23:04:45"),
      (29, "2016-11-14 23:06:45"),
      (30, "2016-11-14 23:08:46"),
      (31, "2016-11-14 23:10:46"),
      (32, "2016-11-14 23:12:46"),
      (33, "2016-11-14 23:14:46"),
      (34, "2016-11-14 23:16:47"),
      (35, "2016-11-14 23:18:47"),
      (36, "2016-11-14 23:20:47"),
      (37, "2016-11-14 23:22:48"),
      (38, "2016-11-14 23:24:48"),
      (39, "2016-11-14 23:26:48"),
      (40, "2016-11-14 23:28:49"),
      (41, "2016-11-14 23:30:49"),
      (42, "2016-11-14 23:32:49"),
      (43, "2016-11-14 23:34:49"),
      (44, "2016-11-14 23:36:50"),
      (45, "2016-11-14 23:38:50"),
      (46, "2016-11-14 23:40:50"),
      (47, "2016-11-14 23:42:51"),
      (48, "2016-11-14 23:44:51"),
      (49, "2016-11-14 23:46:51"),
      (50, "2016-11-14 23:48:52"),
      (51, "2016-11-14 23:50:52"),
      (52, "2016-11-14 23:52:53"),
      (53, "2016-11-14 23:54:52"),
      (54, "2016-11-14 23:56:53"),
      (55, "2016-11-14 23:58:53")
    )

  /** 将轨迹以散点图的形式绘制出来 */
  def plotTheTrace(trailData: Array[Point], labels: Int => String): Unit = {
    println("绘制轨迹散点图")
    import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
    import breeze.plot._

    val longitude = new BDV[Double](trailData.map(_.longitude))
    val latitude = new BDV[Double](trailData.map(_.latitude))

    // 这一段是需要的, 不知道为什么, 官网上也没有介绍
    val a = {
      new BDV[Int](1 to 3 toArray)
    }
    val b = new BDM[Int](3, 3, 1 to 9 toArray)

    val f = Figure()
    val p = f.subplot(0)

    p += plot(longitude, latitude, '+'
      , "red", "轨迹散点图"
      , lines = false, shapes = true, labels = labels
    )
    p.xlabel = "经度"
    p.ylabel = "纬度"
  }

  /**
    * 轨迹预处理
    * ----
    * 输入数据: time, (latitude, longitude)
    * ----
    * 算法:
    * 按时间排序;
    * 每个时间保留一个有效数据
    *
    */


  /**
    * 轨迹点差分统计
    * ----
    * 输入数据: 预处理后的轨迹数据, 数据按时间单调递增
    * ----
    * 算法:
    * step1. 要求数据长度小于5时, 返回空, 结束
    * step2. 二阶差分, 并计算每个轨迹点对应的速度、加速度、转角
    *
    * @param trailData 预处理后的轨迹数据
    * @return 轨迹点差分统计特性 [时间、速度、加速度、转角、时间差]
    */
  def trailDiffStat(trailData: Iterable[Point]): Array[PointDiffStat] = {
    if (trailData.size < 3)
      Array.empty[PointDiffStat]
    else
      trailData.toArray.sliding(3).map {
        // 领先2阶, 领先1阶, 当前
        case Array(p2, p1, p) =>
          val shift1 = p2 --> p1
          val shift = p1 --> p
          // 平均速度
          val speed = shift.speed
          // 平均加速度 --注意这里时间是: (t - t2) / 2
          val accelerateVelocity = shift1.accelerateVelocityTo(shift)
          // 转角 --领先1阶的速度方向到当前速度方向的转角: 范围是[-Pi, Pi]
          val corner = shift1.cornerTo(shift)

          PointDiffStat(p.time, speed, accelerateVelocity, corner, shift.timeDiff, shift.to)
      }.toArray
  }

  /**
    * 异常点过滤
    *
    * @param trailData 差分统计后的数据
    * @return 过滤异常点后的数据
    */
  def trailAbnormalFilter(trailData: Array[PointDiffStat]): Array[PointDiffStat] =
    trailData.filter(pointDiff => pointDiff.accelerateVelocity <= 50.0 && pointDiff.speed <= 1000.0)

  /**
    * 轨迹分离
    * ----
    * 输入数据: 异常点过滤后的数据, 数据按时间单调递增
    * 算法:
    * step1. 如果数据长度小于5, 返回空
    * step2. 根据 [最小运动速度 + 最短静止时长] 判定目标是否处于停留状态
    * step3. 此时剩余记录为运动数据, 根据 [最大无记录时间] + [停留] 分离出多条轨迹.
    *
    * @param trailData       异常点过滤后的数据
    * @param minMovingSpeed  最小运动速度: 如果低于该速度认为目标是静止的: 不同交通工具是不同的, 单位: m/s
    * @param maxStaticPeriod 最大静止时长: 如果静止时间过长则认为目标的这次运动已经结束, 单位: 毫秒
    * @param maxNoRecordTime 最大非运动时间: 如果超出这段时间这认为超出了观测, 此时轨迹自然分离, 单位: 毫秒
    * @return 一条或多条轨迹 [轨迹id, 轨迹]
    */
  def trailDisperse(
                     trailData: Array[PointDiffStat],
                     minMovingSpeed: Double,
                     maxStaticPeriod: Long,
                     maxNoRecordTime: Long
                   ): Array[(Int, ArrayBuffer[PointDiffStat])] = {
    /** 录入数据用的结果 */
    val res: mutable.ArrayBuilder[(Int, ArrayBuffer[PointDiffStat])] =
      scala.collection.mutable.ArrayBuilder.make[(Int, ArrayBuffer[PointDiffStat])]()

    /** 轨迹的id */
    var trailId = 0

    /** 上一条移动轨迹点时间和上一次有记录的时间 --除以2因为是要被减的, 只要不是公元前几万年不会越界 */
    var lastMovingPointTime = Long.MinValue >> 1
    var lastRecordPointTime = Long.MinValue >> 1

    /** 存储每条轨迹的缓存 */
    var arrayBuffer = ArrayBuffer.empty[PointDiffStat]

    /** 数据进入缓存[[arrayBuffer]]的状态监控, 当有数据进入时gate是true, 当arrayBuffer中有数据时才可以录入 */
    var gate = false

    /** 判定上一条轨迹是否结束  --初值[[lastMovingPointTime]]等变量是[[Long.MinValue]], 因此初值是true */
    val trailFinish = (pointDiffStat: PointDiffStat) =>
      (pointDiffStat.time - lastMovingPointTime) >= maxStaticPeriod ||
        (pointDiffStat.time - lastRecordPointTime) >= maxNoRecordTime

    /**
      * 算法:
      * ----
      * 一些条件:
      * 1)轨迹开启状态:
      * 和上一个移动记录的时间差小于等于[[maxStaticPeriod]], 以及和上一个有记录的时间差小于等于[[maxNoRecordTime]];
      * 否则就是轨迹进行状态
      * 2)轨迹点移动状态:
      * 轨迹点速度大于[[minMovingSpeed]];
      * 如果 { 轨迹点移动状态 } 表示轨迹有效, 可以录入
      * 3)录入状态: 门是向[[res]]中录入数据的门, 当且仅当门开启时能录入有效数据数据
      * 初始状态关闭;
      * 每次录入[[res]]结束后, 门关闭;
      * 每次有数据进入[[arrayBuffer]], 门开启
      * 当时轨迹开启状态时如果轨迹点同时处于轨迹点移动状态, 门开启
      *
      * 算法规则:
      * 1)初始状态默认[[lastMovingPointTime]]和[[lastRecordPointTime]]为负无穷, [[gate]]为false
      * 2)遍历所有点, 进行如下算法：
      * 3)如果是轨迹开启状态:
      * 每次录入结束后, 门关闭。
      * 如果[[gate]]开启则录入数据, 关闭门;
      *
      */
    trailData.foreach {
      pointDiffStat =>
        // 轨迹开启状态
        if (trailFinish(pointDiffStat)) {
          // 录入数据并清空缓存
          if (gate) {
            res += Tuple2(trailId, arrayBuffer)
            trailId += 1
            arrayBuffer = ArrayBuffer.empty[PointDiffStat]
            gate = false
          }

          lastRecordPointTime = pointDiffStat.time
          if (pointDiffStat.speed >= minMovingSpeed) {
            lastMovingPointTime = pointDiffStat.time
            arrayBuffer += pointDiffStat
            gate = true
          }
        } else {
          lastRecordPointTime = pointDiffStat.time
          // 有效数据, 往缓存中记录
          if (pointDiffStat.speed >= minMovingSpeed) {
            lastMovingPointTime = pointDiffStat.time
            arrayBuffer += pointDiffStat
            gate = true
          }
        }
    }

    // 最后看看录入数据
    if (gate) {
      res += Tuple2(trailId, arrayBuffer)
      trailId += 1
      arrayBuffer = ArrayBuffer.empty[PointDiffStat]
      gate = false
    }

    /** 过滤掉点数小于5的轨迹 */
    res.result().filter {
      case (_, trail) => trail.length >= 5
    }
  }

  /**
    * 发现每条轨迹的盘旋状态
    * ----
    * 盘旋定义
    * 在较短时间内轨迹形成较大的转角 --从[转角，平均速度的角度看]: 转角超过一定范围，且平均转角速度大于等于某个阈值。
    * 即目标的轨迹形成超过盘旋应有的最小转角, 且这段轨迹的平均转角速度超过最小转角速度[[]]
    *
    * @param trail 轨迹, 默认按时间绝对单调有序排列, 每个轨迹至少5个点，这里不再做判断
    *              盘旋最小转角 --单位: rad, 一般意义上盘旋应该至少为Pi, 否则就只是转完了.
    *              盘旋最小平均角速度 --单位: rad/s, 可以利用盘旋最小转角/最大转一圈用的时间(单位是秒)
    *              有效转角阈值 --单位: rad, 如果绝对值低于该阈值会被认为没有转角, 在求盘旋起止点的时候不会被遍历,
    *              但在加和求平均的时候会被加上.
    */
  def trailFind(
                 trail: ArrayBuffer[PointDiffStat],
                 minHoverCorner: Double,
                 minHoverAvgAngularVelocity: Double,
                 validCornerThreshold: Double
               ): ArrayBuffer[PointDiffStat] = {
    val cumulativeDistribution = trail.map { point => (point.time, point.corner) }.scanLeft((0L, 0.0, -1, 0.0)) {
      case ((_, resCorner, resIndex, _), (time, corner)) =>
        (time, resCorner + corner, resIndex + 1, corner)
    }.tail

    /** 去掉一个最大值点和最小值点, 预防噪声 */
    val oneMaxCornerId = cumulativeDistribution.maxBy(_._4)._3
    val oneMinCornerId = cumulativeDistribution.minBy(_._4)._3
    val cumulativeDistributionFilter = cumulativeDistribution
      .filter {
        case (_, _, index, corner) =>
          scala.math.abs(corner) >= validCornerThreshold &&
            index != oneMinCornerId && index != oneMaxCornerId
      }.map { case (time, resCorner, index, _) => (time, resCorner, index) }
    /** 识别所有的转弯点 --可以包括一些非转弯点 */
    var i = 0
    var hoverIndex = Set.empty[Int]
    val hoverRes = ArrayBuffer.empty[PointDiffStat]
    while (i < cumulativeDistributionFilter.length) {
      val (startTime, startIndex) = (cumulativeDistributionFilter(i)._1,
        cumulativeDistributionFilter(i)._3)
      val startCorner = if (i == 0) 0.0 else cumulativeDistributionFilter(i - 1)._2
      val avgSeries = cumulativeDistributionFilter.drop(i).filter {
        case (time, corner, _) =>
          (time - startTime) / 1000 * minHoverAvgAngularVelocity <= scala.math.abs(corner - startCorner)
      }
      if (avgSeries.nonEmpty) {
        val (_, rightHover, rightIndex) = avgSeries.maxBy(_._2)
        val (_, leftHover, leftIndex) = avgSeries.minBy(_._2)
        val hover = scala.math.max(scala.math.abs(leftHover - startCorner), scala.math.abs(rightHover - startCorner))

        if (hover > minHoverCorner) {
          Range(startIndex, scala.math.max(leftIndex, rightIndex) + 1).foreach {
            index =>
              if (!hoverIndex(index)) {
                hoverIndex += index
                hoverRes += trail(index)
              }
          }
        }
      }
      i += 1
    }

    hoverRes
  }

  def test(trailData: Array[Point]): Unit = {
    /** trailData要求: 每个时间对应唯一一个点且轨迹按时间单调有序 --该函数内不再做判断 */
    /** 确认轨迹中点的条数超过5条, 否则无法支撑后面的计算 */
    //    if(trailData.length <= 5)

    /** 轨迹点差分统计 */
    val statData: Array[PointDiffStat] = trailDiffStat(trailData)

    /** 异常点过滤 */
    val filterData = trailAbnormalFilter(statData)

    /** 轨迹分离 */
    val minMovingSpeed: Double = 5
    val maxStaticPeriod: Long = 30 * 60 * 1000
    val maxNoRecordTime: Long = 30 * 60 * 1000
    val trails = trailDisperse(
      filterData,
      minMovingSpeed,
      maxStaticPeriod,
      maxNoRecordTime
    )

    val minHoverCorner: Double = scala.math.Pi * 1 / 2
    val minHoverAvgAngularVelocity = minHoverCorner / (10 * 60)
    val validCornerThreshold = 10.0 / 180 * scala.math.Pi

    val hoverTrail = trails.flatMap {
      case (trailId, trailPoints) =>
        trailFind(trailPoints, minHoverCorner, minHoverAvgAngularVelocity, validCornerThreshold).map(
          point =>
            (trailId, point)
        )
    }

    plotTheTrace(hoverTrail.filter(_._1 == 0).map(_._2.point), labels = (_: Int) => "a")
  }


  /** 测试[[Point]]和[[com.self.core.trailCorner.models.Shift]]中距离计算、转角计算的准确与否 */
  def testTrailPoint(): Unit = {
    val p0 = Point(-1L, 1.0, 178.0)
    val p1 = Point(0L, 1.0, 179.0)
    val p2 = Point(1L, 0.0, -179.0)
    val p3 = Point(2L, 1.0, -178.0)

    val shift0 = p0 --> p1
    val shift1 = p1 --> p2
    val shift2 = p2 --> p3

    println("179到178划过", (Point(0L, 1.0, 179.0) --> Point(21L, 1.0, 178.0)).shiftOnLongitudes)
    println("-179到179划过", (Point(0L, 1.0, -179.0) --> Point(21L, 1.0, 179.0)).shiftOnLongitudes)
    println("178到179", shift0.shiftOnLongitudes, shift0.shiftOnLatitudes)
    println("179到-179", shift1.shiftOnLongitudes, shift1.shiftOnLatitudes)
    println("shift1的距离", shift1.distance)
    println("shift2的距离", shift1.distance)
    println("shift1到shift2的转角", shift1.cornerTo(shift2))
    println("shift1的速度", shift1.speed)
    println("shift2的速度", shift2.speed)
    println("shift1到shift2的平均时间差", shift1.from.time, shift2.time)
    println("shift1到shift2的平均加速度", shift1.accelerateVelocityTo(shift2))

    println("和网页距离测算对比", "网页: 33.739公里", s"算法: ${
      (Point(0L, 39.03055, 117.4598) --> Point(21L, 39.1815, 117.799)).distance
    }")
    // 0.3392000000000053, 33.739公里
  }

  /** 脚本代码 */
  def zzjzScript(): Unit = {
    import java.sql.Date
    import java.text.SimpleDateFormat

    import com.google.gson.JsonParser
    import com.self.core.featurePretreatment.utils.Tools
    import org.apache.spark.sql.columnUtils.DataTypeImpl._
    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    import org.apache.spark.sql.{DataFrame, Row}

    val z1 = z

    /** 0)平台的系统变量和基本的json配置 */
    val jsonParam = "<#jsonparam#>"
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonParam).getAsJsonObject
    val rddTableName = "<#zzjzRddName#>"

    /** 1)输入数据 */
    val tableName = pJsonParser.get("inputTableName").getAsString
    val rawDataFrame = z1.rdd(tableName).asInstanceOf[DataFrame]
    try {
      rawDataFrame.schema.fieldNames.length
    } catch {
      case _: Exception => throw new Exception(s"在获得数据'$tableName'时失败，请确保您填入的是上一个结点输出的数据")
    }

    /** 2)字段处理 */
    //imsi列名
    val imsiCol = pJsonParser.get("imsiCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
    Tools.columnExists(imsiCol, rawDataFrame, true)
    require(
      rawDataFrame.schema(imsiCol).dataType == StringType,
      s"需要'$imsiCol'是String类型"
    )

    //时间字符串列名及其信息
    val timeCol = pJsonParser.get("timeCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
    Tools.columnExists(timeCol, rawDataFrame, true)
    require(
      rawDataFrame.schema(timeCol).dataType == StringType,
      s"需要'$timeCol'是时间字符串类型"
    )
    val timeFormat = pJsonParser.get("timeFormat").getAsString
    val timeParser = try {
      new SimpleDateFormat(timeFormat)
    } catch {
      case e: Exception => throw new Exception(s"您填入的时间字符串格式: ${timeFormat}可能并不是合法的时间字符串格式, " +
        s"具体信息: ${e.getMessage}")
    }
    // 经纬度
    val latCol = pJsonParser.get("latCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
    Tools.columnExists(latCol, rawDataFrame, true)
    require(
      rawDataFrame.schema(latCol).dataType in "atomic",
      s"需要'$latCol'是能转为数值的原子类型"
    )

    val longCol = pJsonParser.get("longCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
    Tools.columnExists(longCol, rawDataFrame, true)
    require(
      rawDataFrame.schema(longCol).dataType in "atomic",
      s"需要'$longCol'是能转为数值的原子类型"
    )

    /** 根据原数据获得有效记录[imsi, (time, latitude, longitude)] */
    val rdd = rawDataFrame.select(imsiCol, timeCol, latCol, longCol).rdd.map {
      row =>
        var dataValid = true
        if (row.isNullAt(0) || row.isNullAt(1) || row.isNullAt(2) || row.isNullAt(3)) {
          dataValid = false
          (dataValid, ("", Point(-1L, 0.0, 0.0)))
        } else {
          val imsi = row.get(0).toString.trim
          val timeString = row.getAs[String](timeCol)
          if (imsi.trim.isEmpty)
            dataValid = false
          // 以毫秒为单位
          val time = util.Try(timeParser.parse(timeString).getTime).getOrElse {
            dataValid = false
            -1L
          }

          val latitude = util.Try(row.get(2).toString.toDouble).getOrElse {
            dataValid = false
            0.0
          }
          val longitude = util.Try(row.get(3).toString.toDouble).getOrElse {
            dataValid = false
            0.0
          }
          (dataValid, (imsi, Point(time, latitude, longitude)))
        }
    }.filter(_._1).values

    /** 轨迹分离参数 */
    /** 最小移动速度 --m/s */
    val minMovingSpeed: Double = pJsonParser.get("minMovingSpeed").getAsString.toDouble
    /** 单条轨迹最大静止时间 --分钟, 经验是最好5分钟以上 */
    val maxStaticPeriod: Long = (pJsonParser.get("maxStaticPeriod").getAsString.toDouble * 60 * 1000).toLong
    /** 最大无记录时间 --分钟, 经验是最好5分钟以上 */
    val maxNoRecordTime: Long = (pJsonParser.get("maxNoRecordTime").getAsString.toDouble * 60 * 1000).toLong

    /** 盘旋界定参数 */
    /** 最小盘旋角度 --度数 */
    val minHoverCorner: Double = pJsonParser.get("minHoverCorner").getAsString
      .toDouble / 180 * scala.math.Pi
    /** 累计旋转最小盘旋角度所用的最大时间 --单位: 分钟 */
    val maxTimeByMinHover = pJsonParser.get("maxTimeByMinHover").getAsString.toDouble
    val minHoverAvgAngularVelocity = minHoverCorner / (maxTimeByMinHover * 60)
    /** 最小有效转角 --度数 */
    val validCornerThreshold = pJsonParser.get("validCornerThreshold").getAsString.toDouble / 180 * scala.math.Pi

    /** 每个目标分别对每个单条轨迹进行处理 */
    val result = rdd.groupByKey().flatMapValues {
      PointTrails =>
        val points = PointTrails.map(p => (p.time, (p.latitude, p.longitude))).toMap.toArray
          .sortBy(_._1).map {
          case (time, (latitude, longitude)) =>
            Point(time, latitude, longitude)
        }

        /** trailData要求: 每个时间对应唯一一个点且轨迹按时间单调有序 --该函数内不再做判断 */

        /** 轨迹点差分统计 */
        val statData: Array[PointDiffStat] = trailDiffStat(points)

        /** 异常点过滤 */
        val filterData = trailAbnormalFilter(statData)

        /** 轨迹分离 */
        val trails = trailDisperse(
          filterData,
          minMovingSpeed,
          maxStaticPeriod,
          maxNoRecordTime
        )

        val hoverTrail = trails.map {
          case (trailId, trailPoints) =>
            val hover = trailFind(trailPoints, minHoverCorner, minHoverAvgAngularVelocity, validCornerThreshold)
            (trailId, hover)
        }.filter(_._2.length >= 5).flatMap {
          case (trailId, hover) =>
            hover.map {
              point => (trailId, point)
            }
        }

        hoverTrail
    }

    val newDataFrame = rawDataFrame.sqlContext.createDataFrame(
      result.map {
        case (imsi, (trailId, point)) =>
          Row(imsi, trailId, timeParser.format(new Date(point.time)), point.point.latitude, point.point.longitude, point.corner)
      }, StructType(Array(
        StructField("imsi", StringType), StructField("trailId", IntegerType), StructField("time", StringType),
        StructField("latitude", DoubleType), StructField("longitude", DoubleType), StructField("corner", DoubleType)
      )))

    newDataFrame.show()

    newDataFrame.cache()
    newDataFrame.registerTempTable(rddTableName)
    outputrdd.put(rddTableName, newDataFrame)


  }


  override def run(): Unit = {
    //    testTrailPoint()

    //    val timeLabels: Int => String = (index: Int) => trailDataLabels.getOrElse(index, "none")

    //    plotTheTrace(trailData)

    //    val res = scripts()
//    test(trailData)

//    val df = sqlc.createDataFrame(
//      Array(
//        "20180201 00:00:00",
//        "20180201 00:00:00",
//        "2018-02-01 00:00:00",
//        "2018-02-01 00:00:00",
//        "2018-02-01 00:00:00",
//        "20180201 00:00:00",
//        "2018-02-01 00:00:00",
//        "20180201 00:00:00"
//      ).map(Tuple1.apply)
//    ).toDF("cre_date")
//    val rddTableName = "<#zzjzRddName#>"
//    df.registerTempTable(rddTableName)
//    df.cache()
//    outputrdd.put(rddTableName, df)

    import breeze.signal.fourierTr
    import breeze.linalg.DenseVector
    val uu =  fourierTr.apply(new DenseVector(Array(0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0)))
    println(uu)
    println(uu.length)

    import org.apache.spark.sql.functions.unix_timestamp

  }
}
