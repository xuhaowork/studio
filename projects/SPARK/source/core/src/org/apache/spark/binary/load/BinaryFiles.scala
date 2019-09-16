package org.apache.spark.binary.load

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.hadoop.mapreduce.{Job => NewHadoopJob}
import org.apache.spark.SparkContext
import org.apache.spark.binary.split.{PortableDataStream, StreamInputFormat}
import org.apache.spark.rdd.RDD


class BinaryFiles(sc:SparkContext)  {

  var _hadoopConfiguration: Configuration = _
  def hadoopConfiguration: Configuration = _hadoopConfiguration
  var stopped: AtomicBoolean = new AtomicBoolean(false)
  // 如何取hadoop 配置文件的块大小
  def defaultMaxPartitions: Long=134217728

  def assertNotStopped(): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("Cannot call methods on a stopped SparkContext")
    }
  }

  def binaryFiles(
                   path: String,
                   frameSize:Long,
                   splitSize: Long = defaultMaxPartitions): RDD[(String, PortableDataStream)] = {
    assertNotStopped()
    val job = new NewHadoopJob(sc.hadoopConfiguration)

    NewFileInputFormat.setInputPaths(job, path)
    val updateConf = job.getConfiguration
    new BinaryFileRDD(
      sc,
      classOf[StreamInputFormat],
      classOf[String],
      classOf[PortableDataStream],
      updateConf,
      frameSize,
      splitSize).setName(path)
  }

}
