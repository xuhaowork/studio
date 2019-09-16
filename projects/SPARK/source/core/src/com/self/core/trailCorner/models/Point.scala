package com.self.core.trailCorner.models

/**
  * 轨迹点的元数据
  * ----
  *
  * @param time      时间戳 --单位是毫秒
  * @param latitude  纬度 --单位是度
  * @param longitude 经度 --单位是度
  */
case class Point(time: Long, latitude: Double, longitude: Double) {
  def -->(that: Point): Shift = Shift(this, that)
}

/**
  * 位移
  * ----
  * 包括表示方向和距离
  *
  * @param from 出发点
  * @param to   当前点
  * @define shiftOnLatitudes  在纬度方向移动的度数, 由东向西为正
  * @define shiftOnLongitudes 在经度方向移动的度数, 有北向南为正
  * @define distance          距离, 单位是千米
  * @define speed             速度, 单位是米/s
  */
case class Shift(from: Point, to: Point) {
  val time: Long = to.time

  /**
    * 在经度上的移动度数
    * ----
    * 算法:  --要保证180°经线前后依然按照由东向西为正且大小一致
    * asin { sin(l1 - l2) } --外加弧度转换
    */
  val shiftOnLongitudes: Double =
    Math.asin(Math.sin((to.longitude - from.longitude) * Math.PI / 180)) * 180 / Math.PI
  val shiftOnLatitudes: Double = to.latitude - from.latitude

  /** 时间差 --要求当前轨迹点要晚于出发的轨迹点, 单位是毫秒 */
  lazy val timeDiff: Long = {
    require(to.time - from.time > 0, s"出现逻辑异常: 从${from}到${to}的移动中, 后面点的时间'${to.time}'比前面点的时间'${from.time}'早")
    to.time - from.time
  }

  /**
    * 轨迹点的球面距离
    * ----
    * 这里球面距离是指在地球模型表面两点的最近球面距离, 单位是千米
    */
  lazy val distance: Double = {
    val aa = Math.sin(shiftOnLatitudes * Math.PI / 360)
    val bb = Math.sin((to.longitude - from.longitude) * Math.PI / 360)
    Math.sin(
      Math.sqrt(
        aa * aa + Math.cos(from.latitude * Math.PI / 180) * Math.cos(to.latitude * Math.PI / 180) * bb * bb
      )
    ) * 2 * 6378.137
  }

  /**
    * 位移的平均速度
    * ----
    * 单位是m/s
    */
  lazy val speed: Double = distance * 1000000 / timeDiff

  /**
    * 位移平均加速度
    * ----
    * 算法: { (p3 - p2)/(t3 - t2) - (p2 - p1)/(t2 - t1) } * 2 / { t3 - t1 }
    *
    * @param to 下一个位移
    * @return 平均加速度, 单位是m/s²
    */
  def accelerateVelocityTo(to: Shift): Double = {
    require(this.time < to.time, s"出现逻辑异常: 从${this}到${to}的移动中, 后面点的时间'${to.time}'比前面点的时间'${this.time}'早")
    (to.speed - this.speed) * 2000 / (to.time - this.from.time)
  }

  /**
    * 位移转角
    * ----
    * 转角定义:
    * 前后速度向量v1和向量v2所需要的按一定方向旋转的最小的角称为转角, 注意转角不是夹角, 它有顺时针和逆时针两个旋转方向。
    * 范围: 限定转角为[-pi, pi], 其中顺时针转角为负, 逆时针转角为正, 坐标轴x轴为经度, y为纬度。
    * ----
    * 计算:
    * 转角的值计算: acos { v1 * v2 / sqrt(||v1|| * ||v2||) }
    * 转角方向的计算: signum{ v1_x * v2_y - v2_x * v1_y } --在[-pi, pi]内, sin(theta2 - theta1)和theta2 - theta1同号
    * 最终转角计算公式: acos { v1 * v2 / sqrt(||v1|| * ||v2||) }  *  signum{ v1_x * v2_y - v2_x * v1_y }
    *
    * @param to 下一个位移
    * @return 转角, 弧度
    */
  def cornerTo(to: Shift): Double = {
    require(to.time > this.time, "在计算转角过程中出现异常: 上一个位移需要比当前位移时间早")
    val (v1_x, v2_x) = (this.shiftOnLongitudes, to.shiftOnLongitudes)
    val (v1_y, v2_y) = (this.shiftOnLatitudes, to.shiftOnLatitudes)
    val cosine = (v1_x * v2_x + v1_y * v2_y) / Math.sqrt((v1_x * v1_x + v1_y * v1_y) * (v2_x * v2_x + v2_y * v2_y))

    Math.signum(v1_x * v2_y - v2_x * v1_y) * Math.acos(cosine)
  }

}


/**
  * 轨迹点差分统计特性:
  * 轨迹的时间、速度、加速度、转角、和上一条记录的时差
  * @param time 时间, 单位是millisecond的时间戳
  * @param speed 速度, 单位是m/s
  * @param accelerateVelocity 加速度, m/s²
  * @param corner 转角, rad
  * @param timeDiff 时间差, 毫秒
  */
case class PointDiffStat(time: Long, speed: Double, accelerateVelocity: Double, corner: Double, timeDiff: Long, point: Point)
