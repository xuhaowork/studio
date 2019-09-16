package org.apache.spark.binary.load

import java.util.concurrent.atomic.AtomicBoolean

import com.zzjz.deepinsight.utils.ParameterConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.binary.filter.FileFilter
import org.apache.spark.binary.split.{PortableDataStream, StreamInputFormat}
import org.apache.spark.binary.{FileConditionType, FileReadType}
import org.apache.spark.rdd.RDD

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
class BinaryFilterFile(sc: SparkContext) {
  var _hadoopConfiguration: Configuration = _

  def hadoopConfiguration: Configuration = _hadoopConfiguration

  var stopped: AtomicBoolean = new AtomicBoolean(false)
  private var isSplit = true
  private var config: ParameterConfig = _

  // 如何取hadoop 配置文件的块大小
  def defaultMaxPartitions: Long = 134217728

  def assertNotStopped(): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("Cannot call methods on a stopped SparkContext")
    }
  }


  def setIsSplit(isSplit: Boolean) = {
    this.isSplit = isSplit
    this
  }

  def setParameterConfig(config: ParameterConfig) = {
    this.config = config
    this
  }

  private def getFilterPath: Seq[Path] = {
    require(config != null, "未初始化参数配置项!")
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val filePathFilter = new FileFilter {}.initContext(fs)
    val readType = config.getString("readType")
    FileReadType(readType) match {
      case FileReadType.SINGLE_FILE =>
        val filePath = Seq(new Path(config.getString("singleFile")))
        filePathFilter.dirExists(filePath: _*)
        filePath
      case FileReadType.MULTI_FILE =>
        val filePath = config.getSeq[String]("mutiFile").map(new Path(_))
        filePathFilter.dirExists(filePath: _*)
        filePath
      case FileReadType.DIR =>
        val dirs = config.getSeq[String]("dirValues")
        val filterFile = config.getString("filterFile")
        FileConditionType(filterFile) match {
          case FileConditionType.FILE_NMAE_LENGTH =>
            filePathFilter.setRangeConditionMin(config.getString("rangeConditionMin"))
              .setRangeConditionMax(config.getString("rangeConditionMax"))
              .filter(dirs: _*)(filePathFilter.FILE_NAME_LENGTH)
          case FileConditionType.FILE_NAME =>
            filePathFilter.setSingleCondition(config.getString("singleCondition"))
              .filter(dirs: _*)(filePathFilter.FILE_NAME_KEYWORD)
          case FileConditionType.FILE_TYPE =>
            filePathFilter.setMultiCondition(config.getSeq[String]("multiCondition"): _*)
              .filter(dirs: _*)(filePathFilter.FILE_NAME_PREFIX)
          case FileConditionType.FILE_NAME_SEQUENCE =>
            filePathFilter.setMultiCondition(config.getSeq[String]("multiCondition"): _*)
              .setRangeConditionMin(config.getString("rangeConditionMin"))
              .setRangeConditionMax(config.getString("rangeConditionMax"))
              .filter(dirs: _*)(filePathFilter.FILE_NAME_SEQUENCE)
          case FileConditionType.FILE_NAME_TIME =>
            filePathFilter.setSingleCondition(config.getString("singleCondition"))
              .setRangeConditionMin(config.getString("rangeConditionMin"))
              .setRangeConditionMax(config.getString("rangeConditionMax"))
              .filter(dirs: _*)(filePathFilter.FILE_NAME_TIME)
          case FileConditionType.FILE_NAME_REGREX =>
            filePathFilter.setSingleCondition(config.getString("singleCondition"))
              .filter(dirs: _*)(filePathFilter.FILE_NAME_REGREX)
        }
    }
  }


  def binaryFiles(
                   frameSize: Long,
                   splitSize: Long = defaultMaxPartitions): RDD[(String, PortableDataStream)] = {
    assertNotStopped()
    val job = Job.getInstance(sc.hadoopConfiguration)

    val seqPath = getFilterPath
    require(seqPath.nonEmpty, "经过文件名过滤后得到的路径个数为空,请重新设置过滤条件!")

    FileInputFormat.setInputPaths(job, seqPath: _*)
    val updateConf = job.getConfiguration
    new BinaryFileRDD(
      sc,
      classOf[StreamInputFormat],
      classOf[String],
      classOf[PortableDataStream],
      updateConf,
      frameSize,
      splitSize).setIsSplit(isSplit)
  }

}
