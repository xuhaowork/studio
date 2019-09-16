package org.apache.spark.sql.dbPartiiton.jdbc

import com.aliyun.odps.{Odps, TableSchema}
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.data.Record
import com.aliyun.odps.tunnel.InstanceTunnel
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
  * editor:zhu hang
  * date:2018/07/11
  */

private class OdpsQueryPartition(idx: Int, val start: Long, val step: Long) extends Partition {
  override def index: Int = idx

  override def toString: String = "partion:" + idx + " start:" + start + " step:" + step
}

class OdpsQueryRDD[T: ClassTag](@transient sc: SparkContext,
                                accessKeyId: String, accessKey: String, end_point: String, tunnelUrl: String,
                                project: String, instanceId: String, part: String, numPartition: Int,
                                mapRow: (Record, TableSchema) => T = OdpsQueryRDD.recordToObjectArray _) extends RDD[T](sc, Nil) {

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = {
    new NextIterator[T] {
      val odpsQueryPartition = thePart.asInstanceOf[OdpsQueryPartition]
      val downloadSession = getSession()
      val recordReader = downloadSession.openRecordReader(odpsQueryPartition.start, odpsQueryPartition.step)
      val schema = downloadSession.getSchema

      override protected def getNext(): T = {
        val r = recordReader.read()
        if (r != null) {
          mapRow(r, schema)
        } else {
          finished = true
          null.asInstanceOf[T]
        }
      }

      override protected def close() = recordReader.close()
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val session = getSession()
    val dlCount = session.getRecordCount
    val partitionNo = if (dlCount < numPartition) {
      dlCount.toInt
    } else {
      numPartition
    }
    val stepList = getFetchRange(dlCount, partitionNo)
    val partitionRange = Array.tabulate(partitionNo) {
      index => {
        val (start, step) = stepList(index)
        new OdpsQueryPartition(index, start, step)
      }
    }.filter(_.step > 0).map(p => p.asInstanceOf[Partition])
    partitionRange
  }

  def getFetchRange(count: Long, partNo: Int): List[(Long, Long)] = {
    val step = count / partNo
    val left = count - partNo * step
    var stepList: List[(Long, Long)] = Nil
    for (i <- 0 until (partNo)) {
      if (i == partNo - 1)
        stepList = stepList :+ (step * i, step + left)
      else
        stepList = stepList :+ (step * i, step)
    }
    stepList
  }

  def getOdpsSchcma() = {
    val session = getSession()
    session.getSchema
  }

  def getSession() = {
    val account = new AliyunAccount(accessKeyId, accessKey)
    val odps = new Odps(account)
    odps.setEndpoint(end_point)
    odps.setDefaultProject(project)
    val tunnel = new InstanceTunnel(odps)
    tunnel.setEndpoint(tunnelUrl)
    val downloadSession = tunnel.createDownloadSession(project, instanceId)
    downloadSession
  }
}

object OdpsQueryRDD {
  def recordToObjectArray(rec: Record, tableSchema: TableSchema): Array[AnyRef] = {
    Array.tabulate[AnyRef](rec.getColumnCount) { i =>
      val colType = tableSchema.getColumn(i).getTypeInfo.getTypeName
      colType match {
        case "BIGINT" => rec.getBigint(i)
        case "DOUBLE" => rec.getDouble(i)
        case "BOOLEAN" => rec.getBoolean(i)
        case "DATETIME" | "STRING" => rec.getString(i)
        case _ => throw new Exception(s"目前不支持数据类型【${colType}】")
      }
    }
  }
}

