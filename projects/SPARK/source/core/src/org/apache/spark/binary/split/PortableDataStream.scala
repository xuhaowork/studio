package org.apache.spark.binary.split

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.spark.annotation.Experimental
import org.apache.spark.deploy.SparkHadoopUtil


private[spark] abstract class StreamFileInputFormat[T]
  extends CombineFileInputFormat[String, T]
{
  private var isSplit=true
  override protected def isSplitable(context: JobContext, file: Path): Boolean = isSplit


  def createRecordReader(split: InputSplit, taContext: TaskAttemptContext): RecordReader[String, T]

  def setIsSplit(isSplit:Boolean) ={
    this.isSplit =isSplit
    this
  }



}

private[spark] abstract class StreamBasedRecordReader[T](
    split: CombineFileSplit,
    context: TaskAttemptContext,
    index: Integer)
  extends RecordReader[String, T] {

  // True means the current file has been processed, then skip it.
  private var processed = false

  private var key = ""
  private var value: T = null.asInstanceOf[T]

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}
  override def close(): Unit = {}

  override def getProgress: Float = if (processed) 1.0f else 0.0f

  override def getCurrentKey: String = key

  override def getCurrentValue: T = value

  override def nextKeyValue: Boolean = {
    if (!processed) {
      val fileIn = new PortableDataStream(split, context, index)
      value = parseStream(fileIn)
      fileIn.close() // if it has not been open yet, close does nothing
      key = fileIn.getPath
      processed = true
      true
    } else {
      false
    }
  }

  /**
   * Parse the stream (and close it afterwards) and return the value as in type T
   * @param inStream the stream to be read in
   * @return the data formatted as
   */
  def parseStream(inStream: PortableDataStream): T
}

/**
 * Reads the record in directly as a stream for other objects to manipulate and handle
 */
private[spark] class StreamRecordReader(
    split: CombineFileSplit,
    context: TaskAttemptContext,
    index: Integer)
  extends StreamBasedRecordReader[PortableDataStream](split, context, index) {

  def parseStream(inStream: PortableDataStream): PortableDataStream = inStream
}

/**
 * The format for the PortableDataStream files
 */
private[spark] class StreamInputFormat extends StreamFileInputFormat[PortableDataStream] {
  override def createRecordReader(split: InputSplit, taContext: TaskAttemptContext)
    : CombineFileRecordReader[String, PortableDataStream] = {
    new CombineFileRecordReader[String, PortableDataStream](
      split.asInstanceOf[CombineFileSplit], taContext, classOf[StreamRecordReader])
  }
}

/**
 * A class that allows DataStreams to be serialized and moved around by not creating them
 * until they need to be read
 * @note TaskAttemptContext is not serializable resulting in the confBytes construct
 * @note CombineFileSplit is not serializable resulting in the splitBytes construct
 */
@Experimental
class PortableDataStream(
    @transient isplit: CombineFileSplit,
    @transient context: TaskAttemptContext,
    index: Integer)
  extends Serializable {

  // transient forces file to be reopened after being serialization
  // it is also used for non-serializable classes

  @transient private var fileIn: DataInputStream = null
  @transient private var isOpen = false


  private val confBytes = {
    val baos = new ByteArrayOutputStream()
    context.getConfiguration.write(new DataOutputStream(baos))
    baos.toByteArray
  }

  private val splitBytes = {
    val baos = new ByteArrayOutputStream()
    isplit.write(new DataOutputStream(baos))
    baos.toByteArray
  }

  @transient private lazy val split = {
    val bais = new ByteArrayInputStream(splitBytes)
    val nsplit = new CombineFileSplit()
    nsplit.readFields(new DataInputStream(bais))
    nsplit
  }

  @transient private lazy val conf = {
    val bais = new ByteArrayInputStream(confBytes)
    val nconf = new Configuration()
    nconf.readFields(new DataInputStream(bais))
    nconf
  }
  /**
   * Calculate the path name independently of opening the file
   */
  @transient private lazy val path = {
    val pathp = split.getPath(0)
    pathp.toString
  }

  /**
   * Create a new DataInputStream from the split and context
   */
  def open(): DataInputStream = {
    if (!isOpen) {
      val pathp = split.getPath(0)

      val fs = pathp.getFileSystem(conf)
      fileIn = fs.open(pathp)
      isOpen = true
    }
    fileIn
  }

  /**
    *  Get   file  offset
   */

  def getOffset():Long={
    split.getOffset(0)
  }

  def  getLength():Long={
    split.getLength(0)
  }
  /**
      Get   split  length
    */



  /**
   * Read the file as a byte array
   */
 /* def toArray(): Array[Byte] = {
    open()
    val innerBuffer = ByteStreams.toByteArray(fileIn)

    close()
    innerBuffer
  }*/

  def  toArray():Array[Byte]={
   val offset=split.getOffset(0)
   val length=split.getLength(0).toInt
   val innerBuffer=new  Array[Byte](length)
   open()

   try{
     fileIn.skip(offset)
     fileIn.readFully(innerBuffer)
   }catch {
     case _:Throwable=> 
   }
   close()
   innerBuffer
 }




  /**
   * Close the file (if it is currently open)
   */
  def close(): Unit = {
    if (isOpen) {
      try {
        fileIn.close()
        isOpen = false
      } catch {
        case ioe: java.io.IOException => // do nothing
      }
    }
  }

  def getPath(): String = path
}

