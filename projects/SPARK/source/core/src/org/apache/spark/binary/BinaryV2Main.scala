package org.apache.spark.binary

import com.jayway.jsonpath.JsonPath
import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.utils.{NumericType, ParameterConfig, ToolUtils}
import org.apache.spark.binary.load.BinaryFilterFile
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

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
class BinaryV2Main extends BaseMain {
  override def run() = {
    val jsonparam = "<#zzjzParam#>"
    val ctxRead = JsonPath.parse(jsonparam)
    val config = new ParameterConfig

    val isSplitable =
      ToolUtils.toType[Boolean](ctxRead.read[String]("$.isSplitable.value"))

    val splitSize = if (isSplitable) {
      ToolUtils.toSignType[Long](ctxRead.read[String]("$.isSplitable.splitSize"))(NumericType.POSITIVE)
    } else 0
    val frameSize = if (isSplitable)
      ToolUtils.toSignType[Int](ctxRead.read[String]("$.isSplitable.frameSize"))(NumericType.POSITIVE)
    else -1L
    if (frameSize > splitSize) {
      new IllegalArgumentException("输入的参数交叠字节数一定小于等于切片的字节大小!")
    }

    val readType = ctxRead.read[String]("$.readType.value")

    import scala.collection.JavaConversions._
    FileReadType(readType) match {
      case FileReadType.DIR =>
        val dirType = ctxRead.read[String]("$.readType.dirType.value")
        config.set("dirType", dirType)
        val dirValues = FileReadType(dirType) match {
          case FileReadType.MANUAL =>
            ctxRead.read[String]("$.readType.dirType.manualVal").split("\\n").toSeq
          case FileReadType.CONTROL =>
            ctxRead.read[java.util.List[String]]("$.readType.dirType.controlVal[*].path").toSeq
        }
        config.set("dirValues", dirValues.distinct)
        // 计算过滤条件
        val filterFile = ctxRead.read[String]("$.readType.filterFile.value")
        config.set("filterFile", filterFile)
        FileConditionType(filterFile) match {
          case FileConditionType.FILE_NMAE_LENGTH =>
            val fileNameLenVal = ctxRead.read[java.util.List[String]]("$.readType.filterFile.fileNameLenVal[*]").toSeq
            //判断范围是否满足
            val minLen = ToolUtils.toSignType[Int](String.valueOf(fileNameLenVal.head))(NumericType.POSITIVE)
            val maxLen = ToolUtils.toSignType[Int](String.valueOf(fileNameLenVal.last))(NumericType.POSITIVE)
            config.set("rangeConditionMin", minLen.toString).set("rangeConditionMax", maxLen.toString)

          case FileConditionType.FILE_NAME =>
            val fileNameVal = ctxRead.read[String]("$.readType.filterFile.fileNameVal")
            config.set("singleCondition", fileNameVal)
          case FileConditionType.FILE_TYPE =>
            val fileTypeVal = ctxRead.read[String]("$.readType.filterFile.fileTypeVal").split(",").toSeq
            config.set("multiCondition", fileTypeVal)
          case FileConditionType.FILE_NAME_SEQUENCE =>

            val fileNameWithSeqSep = ctxRead.read[java.util.List[String]]("$.readType.filterFile.fileNameWithSeqSep1[*].fileNameWithSeqSep").toSeq
            val fileNameWithSeqVal = ctxRead.read[java.util.List[String]]("$.readType.filterFile.fileNameWithSeqVal[*]").toSeq
            if (fileNameWithSeqSep.isEmpty)
              throw new IllegalArgumentException("使用序列号过滤文件时，文件名与序列号之间的连接符未指定！")
            //判断范围是否满足
            val minLen = ToolUtils.toSignType[Int](String.valueOf(fileNameWithSeqVal.head))(NumericType.POSITIVE).toString
            val maxLen = ToolUtils.toSignType[Int](String.valueOf(fileNameWithSeqVal.last))(NumericType.POSITIVE).toString
            config.set("rangeConditionMin", minLen).set("rangeConditionMax", maxLen).set("multiCondition", fileNameWithSeqSep)
          case FileConditionType.FILE_NAME_TIME =>
            val fileNameWithTimePattern = ctxRead.read[String]("$.readType.filterFile.fileNameWithTimePattern")
            val fileNameWithTimeVal = ctxRead.read[java.util.List[String]]("$.readType.filterFile.fileNameWithTimeVal[*]").toSeq

            val startTime = DateTime.parse(fileNameWithTimeVal.head, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
            val endTime = DateTime.parse(fileNameWithTimeVal.last, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
            if (startTime.isAfter(endTime))
              throw new IllegalArgumentException("基于文件名带时间进行过滤时参数范围设置不正确,最小长度大于了最大长度！")
            config.set("rangeConditionMin", fileNameWithTimeVal.head)
              .set("rangeConditionMax", fileNameWithTimeVal.last)
              .set("singleCondition", fileNameWithTimePattern)

          case FileConditionType.FILE_NAME_REGREX =>
            val regrexVal = ctxRead.read[String]("$.readType.filterFile.regrexVal")
            config.set("singleCondition", regrexVal)

        }
      case FileReadType.SINGLE_FILE =>
        val singleFile = ctxRead.read[String]("$.readType.singleFile")
        config.set("singleFile", singleFile)
      case FileReadType.MULTI_FILE =>
        val mutiFile = ctxRead.read[java.util.List[String]]("$.readType.mutiFile[*].path").toSeq
        config.set("mutiFile", mutiFile)
    }

    config.set("isSplitable", isSplitable)
      .set("splitSize", splitSize)
      .set("frameSize", frameSize)
      .set("readType", readType)

    val binaryFilterFile = new BinaryFilterFile(sc)
      .setIsSplit(isSplitable)
      .setParameterConfig(config)
      .binaryFiles(frameSize, splitSize)

    val rdd1 = binaryFilterFile.map(x => (x._1, x._2.getOffset(), x._2.getLength(), x._2.toArray()))
    import sqlc.implicits._
    val outDf = rdd1.toDF("fileName", "offset", "length", "arrayByte")
    outDf.show()
    outputrdd.put("<#rddtablename#>", outDf)
    outDf.registerTempTable("<#rddtablename#>")


  }
}
