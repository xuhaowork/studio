package org.apache.spark.binary.load

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.spark.binary.split.StreamFileInputFormat
import org.apache.spark.rdd.{NewHadoopPartition, NewHadoopRDD}
import org.apache.spark.{Partition, SparkContext}

class BinaryFileRDD[T](
                                       sc: SparkContext,
                                       inputFormatClass: Class[_ <: StreamFileInputFormat[T]],
                                       keyClass: Class[String],
                                       valueClass: Class[T],
                                       @transient conf: Configuration,
                                       frameSize:Long,
                                       maxSize: Long)
  extends NewHadoopRDD[String, T](sc, inputFormatClass, keyClass, valueClass, conf) {

  private var isSplit:Boolean=_
  def setIsSplit(isSplit:Boolean)= {
    this.isSplit=isSplit
    this
  }

  override def getPartitions: Array[Partition] = {
    val inputFormat = inputFormatClass.newInstance
    inputFormat.setIsSplit(isSplit)
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }
    val jobContext = new JobContextImpl(conf, jobId)

    inputFormat.setFrameSize(frameSize)
    inputFormat.setInitSize(maxSize)

    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }
}

