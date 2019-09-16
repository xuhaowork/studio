package com.self.core.timeBinning.models

import com.self.core.utils.TimeColInfo

/**
  *
  * @param format                显示方式包括自适应和设定显示方式两种 --selfAdaption, design 不可变
  * @param colToBePresented      要展示的列 可变
  * @param binningTagFormat      标记每个分箱的方式包括三种 --以开始时间标记 byStart, 以中间时间标记 byMiddle, 以区间标记byInterval 不可变
  * @param extraColToBePresented 如果是二级分箱则需要多一级的列信息 可变
  */
class PresentInfo(val format: String,
                  val binningTagFormat: String,
                  val colToBePresented: TimeColInfo,
                  var extraColToBePresented: Option[TimeColInfo]) extends {
  val timeFormat: String = "yyyy-MM-dd HH:mm:ss"

  /**
    * 结合初始时间列的信息和分箱信息设计展示的方式
    * ----
    * 宗旨是在udf之前彻底解决好分箱列（以及二级分箱列）的显示问题
    * ----
    * 由于时间信息较多统一按字符串"yyyy-MM-dd HH:mm:ss"方式展示
    * 0.如果是二级分箱自动选择自适应显示
    * 1.自适应显示
    * 1)等宽分箱
    * 不按区间展示时 --和初始的显示方式保持一致
    * 按区间展示是 --字段类型变为pasteString形式，如果如果原有类型是string，不变直接进行paste，如果不是选择[[timeFormat]]后进行paste
    * 2)自然时间分箱 统一以string形式显示
    * 如果是低级时间分箱按较低级的时间字符串展示
    * 其他的按照[[timeFormat]]的对应精度显示
    * 2.其他
    * 不按区间展示时 --和初始的显示方式保持一致
    * 按区间展示是 --pasteString形式，如果如果原有类型是string，不变直接进行paste，如果不是选择[[timeFormat]]后进行paste
    *
    * @param timeColInfo 初始时间列的信息
    * @param binningInfo 分箱信息
    * @return
    */
  def selfAdapt(timeColInfo: TimeColInfo, binningInfo: BinningInfo): this.type = {
    val logic = binningInfo match {
      case info: NaturalInfo => info.twoGrade
      case _ => false
    }
    if (logic || format == "selfAdaption") { // 如果是自适应，按照上述策略进行调整，生成新的展示方式
      binningInfo match {
        case _: FixedLengthInfo => // 如果是等宽时间分箱，需要和输入的原始时间列保持一致
          this.colToBePresented.updateTimeFormat(timeColInfo.dataType, timeColInfo.timeFormat)
          this
        case info: NaturalInfo => // 自然时间分箱统一按字符串形式显示
          val originString = if (timeColInfo.dataType == "long") "" else timeColInfo.timeFormat.getOrElse("")

          if (!info.twoGrade) {
            colToBePresented.updateTimeFormat("string", Some(unitMap(info.slidingUnit, originString))) // 变更列类型
            this
          } else {
            require(extraColToBePresented.isDefined, "error202，参数设定错误: 分箱信息为二级分箱时需要二级分箱的列名")
            val firstGradeFormat = unitMap(info.slidingUnit, originString)
            val secondGradeFormat = unitExtraMap(info.slidingUnit, originString)

            colToBePresented.updateTimeFormat("string", Some(firstGradeFormat)) // 变更第一列类型
            extraColToBePresented = Some(extraColToBePresented.get.updateTimeFormat("string", Some(secondGradeFormat)))
            this
          }
      }
    } else {
      if(binningTagFormat == "byInterval")
        colToBePresented.updateTimeFormat("pasteString", colToBePresented.timeFormat) // 将显示方式变为pasteString
      this // 如果不是自适应，就按照输入的展示策略进行展示
    }


  }


  private def unitMap(unit: String, originString: String): String =
    if (originString.toLowerCase().trim.startsWith("hh")
      && originString.forall(cha => !(Array('Y', 'y', 'M', 'D', 'd') contains cha))
      && originString.trim.length <= 8) { // 过滤一些低级时间格式
      unit match {
        case "year" => throw new Exception("您输入的初始时间类型中不包含年份月份日期等信息，需要选择更小的分箱单位")
        case "season" => throw new Exception("您输入的初始时间类型中不包含年份月份日期等信息，需要选择更小的分箱单位")
        case "month" => throw new Exception("您输入的初始时间类型中不包含年份月份日期等信息，需要选择更小的分箱单位")
        case "week" => throw new Exception("您输入的初始时间类型中不包含年份月份日期等信息，需要选择更小的分箱单位")
        case "day" => throw new Exception("您输入的初始时间类型中不包含年份月份日期等信息，需要选择更小的分箱单位")
        case "hour" => "HH时"
        case "minute" => "HH时mm分"
        case "second" => "HH时mm分ss秒"
      }
    } else {
      unit match {
        case "year" => "yyyy"
        case "season" => "yyyy-MM"
        case "month" => "yyyy-MM"
        case "week" => "yyyy-MM-dd EEE"
        case "day" => "yyyy-MM-dd"
        case "hour" => "yyyy-MM-dd HH"
        case "minute" => "yyyy-MM-dd HH:mm"
        case "second" => "yyyy-MM-dd HH:mm:ss"
      }
    }


  private def unitExtraMap(unit: String, originString: String = "yyyy-MM-dd HH:mm:ss"): String =
    if (originString.toLowerCase().trim.startsWith("hh")
      && originString.forall(cha => !(Array('Y', 'y', 'M', 'D', 'd') contains cha))
      && originString.trim.length <= 8) { // 过滤一些低级时间格式，该时间只能以更低的时间单位分箱，并且显示
      unit match {
        case "year" => throw new Exception("您输入的初始时间类型中不包含年份月份日期等信息，需要选择更小的分箱单位")
        case "season" => throw new Exception("您输入的初始时间类型中不包含年份月份日期等信息，需要选择更小的分箱单位")
        case "month" => throw new Exception("您输入的初始时间类型中不包含年份月份日期等信息，需要选择更小的分箱单位")
        case "week" => throw new Exception("您输入的初始时间类型中不包含年份月份日期等信息，需要选择更小的分箱单位")
        case "day" => throw new Exception("您输入的初始时间类型中不包含年份月份日期等信息，需要选择更小的分箱单位")
        case "hour" => "mm分ss秒"
        case "minute" => "ss秒"
        case "second" => throw new Exception("您输入的单位过小，二级分箱单位最小为分钟")
      }
    } else {
      if (originString.length <= 10 && originString.trim.length >= 1) { //如果是空字符串默认以一般形式输出
        unit match {
          case "year" => "MM-dd"
          case "season" => "MM-dd"
          case "month" => "dd日"
          case "week" => "EEE"
          case "day" => "HH:mm:ss"
          case "hour" => "mm:ss"
          case "minute" => "ss"
          case "second" => throw new Exception("您输入的单位过小，二级分箱单位最小为分钟")
        }
      } else { // 一般形式输出
        unit match {
          case "year" => "MM-dd HH:mm:ss"
          case "season" => "MM-dd HH:mm:ss"
          case "month" => "dd日 HH:mm:ss"
          case "week" => "EEE HH:mm:ss"
          case "day" => "HH:mm:ss"
          case "hour" => "mm:ss"
          case "minute" => "ss"
          case "second" => throw new Exception("您输入的单位过小，二级分箱单位最小为分钟")
        }
      }
    }


}

/**
  * @param timeFormat         要展示的列的时间格式
  * @param extraColTimeFormat 如果是二级分箱则需要给出多一级的列的信息
  */
case class PresentFormat(timeFormat: String, extraColTimeFormat: Option[String])
