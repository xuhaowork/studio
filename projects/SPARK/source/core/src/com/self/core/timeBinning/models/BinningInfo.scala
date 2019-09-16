package com.self.core.timeBinning.models

/**
  * 分箱信息
  */
trait BinningInfo

/**
  * 分为三种
  * ----
  * 由于分箱信息的参数是外来的超参数，参数设定为不可变的
  * ----
  * 1）固定长度 需要有参数length
  * 2）自然时间单位分箱一级分箱  需要有分箱的单位
  * 3）自燃时间单位分箱二级分箱  需要有一级分箱单位和二级分箱单位
  */
class FixedLengthInfo(val length: Long, val startTimeStamp: Long) extends BinningInfo

class NaturalInfo(val slidingUnit: String, val twoGrade: Boolean = false) extends BinningInfo {
  /**
    * 分箱级别降一级，当twoGrade时使用 --将一级分级信息认为是false
    *
    * @return
    */
  def degrade(): NaturalInfo = {
    require(twoGrade, "当自然分箱为二级分箱时才能使用")
    val newUnit = slidingUnit match {
      case "year" => "month"
      case "season" => "month"
      case "month" => "day"
      case "week" => "day" // 需要以周期显示
      case "day" => "hour"
      case "hour" => "minute"
      case "minute" => "second"
      case "second" => throw new Exception(s"error201，参数异常：您输入的一级分箱单位${slidingUnit}过小，已经找不到下一级单位。")
      case _ => throw new Exception(s"error201，参数异常：自然时间分箱的单位不在给定格式中。")
    }
    new NaturalInfo(newUnit, false) // 设定为false标识降级后又变为一级分箱了，后面递归会调用，该代码时要注意别造成无限递归
  }


}

