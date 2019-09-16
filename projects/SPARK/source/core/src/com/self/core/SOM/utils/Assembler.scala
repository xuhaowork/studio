package com.self.core.SOM.utils

import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors, Vector}

import scala.collection.mutable

object Assembler extends Serializable {
  def assemble(vv: Any*): Vector = {
    val indices = mutable.ArrayBuilder.make[Int]
    val values = mutable.ArrayBuilder.make[Double]
    var cur = 0
    vv.foreach {
      case v: Double =>
        if (v != 0.0) {
          indices += cur
          values += v
        }
        cur += 1

      case i: Int =>
        if (i != 0) {
          indices += cur
          values += i.toDouble
        }
        cur += 1

      case i: Float =>
        if (i != 0) {
          indices += cur
          values += i.toDouble
        }
        cur += 1

      case s: String =>
        val i = try {
          s.toDouble
        } catch {
          case e: Exception => throw new SparkException(s"您输入的数据${s}在转为double类型时失败，" +
            s"具体信息:${e.getMessage}")
        }
        if (i != 0) {
          indices += cur
          values += i.toDouble
        }
        cur += 1

      case vec: DenseVector =>
        var i = 0
        val localValuesSize = vec.values.length
        val localValues = vec.values

        while (i < localValuesSize) {
          if (localValues(i) != 0.0) {
            indices += cur + i
            values += localValues(i)
          }
          i += 1
        }
        cur += vec.size

      case vec: SparseVector =>
        var i = 0
        val localValuesSize = vec.values.length
        val localValues = vec.values
        val localIndices = vec.indices

        while (i < localValuesSize) {
          if (localValues(i) != 0.0) {
            indices += cur + localIndices(i)
            values += localValues(i)
          }
          i += 1
        }
        cur += vec.size

      case arr: Seq[Any] =>
        arr.foreach { element =>
          val value = try {
            element.toString.toDouble
          } catch {
            case e: Exception => throw new Exception(s"您输入的数组${arr.mkString(",")}中的数据${element}" +
              s"在转为数值类型时失败，具体信息${e.getMessage}")
          }
          indices += cur
          values += value
          cur += 1
        }
      case null =>
        // TODO: output Double.NaN?
        throw new SparkException("null值不能被集成到向量中")
      case o =>
        throw new SparkException(s"$o of type ${o.getClass.getName} 不能被集成到向量中")
    }

    Vectors.sparse(cur, indices.result(), values.result()).compressed
  }


}
