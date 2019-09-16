package org.apache.spark.binary.filter

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, PathFilter}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.matching.Regex


/**
  *
  * Author shifeng
  * Date 2018/11/28
  * Version 1.0
  * Update 
  *
  * Desc
  *
  */
abstract class FileFilter {
  //  多个目录、条件设定
  private[this] var singleCondition: String = _
  private[this] var multiCondition: Seq[String] = _
  private[this] var rangeConditionMin: String = _
  private[this] var rangeConditionMax: String = _
  private[this] var fs: FileSystem = _

  /**
    * 用户初始化  FileSystem
    *
    * @param fs FileSystem实例对象
    * @return
    */
  def initContext(fs: FileSystem) = {
    this.fs = fs
    this
  }

  def setSingleCondition(singleCondition: String) = {
    this.singleCondition = singleCondition
    this
  }

  def getSingleCondition = singleCondition


  def setMultiCondition(multiCondition: String*) = {
    this.multiCondition = multiCondition
    this
  }

  def getMultiCondition = multiCondition

  def setRangeConditionMin(rangeConditionMin: String) = {
    this.rangeConditionMin = rangeConditionMin
    this
  }

  def getRangeConditionMin = rangeConditionMin

  def setRangeConditionMax(rangeConditionMax: String) = {
    this.rangeConditionMax = rangeConditionMax
    this
  }

  def getRangeConditionMax = rangeConditionMax

  def dirExists(pathes: Path*) = {
    require(pathes.nonEmpty, "指定的路径为空!")
    for (path <- pathes) {
      require(fs.exists(path), s"文件系统中指定的路径:${path.getName} 不存在!")
    }
  }

  /**
    * 指定多个目录，根据目录和条件对其进行
    *
    * @param  dirs 多个目录
    * @return 返回满足条件的路径
    */
  def filter(dirs: String*)(toBoolean: String => Boolean): Seq[Path] = {
    // 判断指定的目录是否都存在，不存在则产生异常
    val dirPath = dirs.map(new Path(_))
    dirExists(dirPath: _*)
    val fileStatus = fs.listStatus(dirPath.toArray, new PathFilter {
      override def accept(path: Path) = {
        toBoolean(path.getName)
      }
    })
    FileUtil.stat2Paths(fileStatus)
  }


  /**
    * 定义各种过滤条件
    */

  /**
    * 根据文件名的长度来进行过滤
    */
  val FILE_NAME_LENGTH: String => Boolean = (fileName: String) => {
    require(rangeConditionMin != null && rangeConditionMax != null, "文件名的长度参数范围未被初始化!")
    val lastDot = fileName.lastIndexOf(".")
    val fileNameNoPrefix = if (lastDot == -1) fileName else fileName.substring(0, lastDot)
    rangeConditionMin.toInt <= fileNameNoPrefix.length && fileNameNoPrefix.length <= rangeConditionMax.toInt
  }

  /**
    * 根据文件的后缀名来进行过滤
    */
  val FILE_NAME_PREFIX: String => Boolean = (fileName: String) => {
    require(multiCondition.nonEmpty || multiCondition != null, "文件的后缀名参数未被初始化!")
    val lastDot = fileName.lastIndexOf(".")
    if (lastDot == -1) {
      false
    } else {
      val prefix = fileName.substring(lastDot + 1).toUpperCase
      multiCondition.map(_.toUpperCase).contains(prefix)
    }
  }


  /**
    * 根据文件名关键字模糊匹配
    */
  val FILE_NAME_KEYWORD: String => Boolean = (fileName: String) => {
    require(singleCondition != null, "模糊匹配参数未被初始化!")
    val lastDot = fileName.lastIndexOf(".")
    if (lastDot == -1) fileName.toUpperCase.contains(singleCondition.toUpperCase)
    else fileName.substring(0, lastDot).toUpperCase.contains(singleCondition.toUpperCase)
  }

  /**
    * 根据带有文件名的时间来进行筛选
    */
  val FILE_NAME_TIME: String => Boolean = (fileName: String) => {
    require(singleCondition != null, "时间匹配规则参数未被初始化!")
    require(rangeConditionMin != null && rangeConditionMax != null, "带有文件名的时间参数范围未被初始化!")

    val lastDot = fileName.lastIndexOf(".")
    val fileNameNoPrefix = if (lastDot == -1) fileName else fileName.substring(0, lastDot)

    val fileDateTime: DateTime = parserTime(singleCondition, fileNameNoPrefix)
    if (fileDateTime == null)
      false
    else {
      val startTime = DateTime.parse(rangeConditionMin, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
      val endTime = DateTime.parse(rangeConditionMax, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))

      startTime.isEqual(fileDateTime) ||
        endTime.isEqual(fileDateTime) ||
        (startTime.isBefore(fileDateTime) && endTime.isAfter(fileDateTime))
    }


    // 再判断是否在指定的时间范围内
  }
  val parserTime: (String, String) => DateTime = (filePattren: String, fileName: String) => {
    try {
      val regex = """.*\{(.*)\}.*""".r
      val startLabel = filePattren.indexOf("{")
      val endLabel = filePattren.indexOf("}")
      val tableBuffer = fileName.toBuffer
      tableBuffer.insert(startLabel, '{')
      tableBuffer.insert(endLabel, '}')
      val tableString = tableBuffer.mkString
      val formatStrRule = filePattren match {
        case regex(bc) => bc
      }
      val fmt = DateTimeFormat.forPattern(formatStrRule)
      val newRule = filePattren.replace("{" + formatStrRule + "}","""\{(.*)\}""")
      val pattern = new Regex(newRule)
      val formatStr = tableString match {
        case pattern(bc) => bc
      }
      DateTime.parse(formatStr, fmt)
    } catch {
      case ex: IllegalArgumentException => null //throw new Exception("匹配规则输11入有误")
      case ex: IndexOutOfBoundsException => null
      case ex: MatchError => null
    }
  }


  /**
    * 根据带有序列的文件名来进行过滤
    */
  val FILE_NAME_SEQUENCE: String => Boolean = (fileName: String) => {
    require(multiCondition.nonEmpty || multiCondition != null, "带有序列的文件名分隔符参数未被初始化!")
    require(rangeConditionMin != null && rangeConditionMax != null, "带有文件名的序列参数范围未被初始化!")
    val lastDot = fileName.lastIndexOf(".")
    val fileNameNoPrefix = if (lastDot == -1) fileName else fileName.substring(0, lastDot)

    val loop = new scala.util.control.Breaks()
    import loop.{break, breakable}
    var fileSeq: Long = -999L
    for (sepChar <- multiCondition) {
      breakable {
        val lastSeq = StringUtils.split(fileNameNoPrefix, sepChar).last
        if (StringUtils.isNumeric(lastSeq)) {
          fileSeq = lastSeq.toLong
          break
        }
      }
    }
    if (fileSeq == -999L) false else rangeConditionMin.toLong <= fileSeq && fileSeq <= rangeConditionMax.toLong
  }


  /**
    * 根据正则表达式来进行过滤数据
    */
  val FILE_NAME_REGREX: String => Boolean = (fileName: String) => {
    require(singleCondition != null, "正则表达式参数未被初始化!")
    val lastDot = fileName.lastIndexOf(".")
    val fileNameNoPrefix = if (lastDot == -1) fileName else fileName.substring(0, lastDot)
    fileNameNoPrefix.matches(singleCondition)
  }


}
