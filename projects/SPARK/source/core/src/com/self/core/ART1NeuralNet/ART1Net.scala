package com.self.core.ART1NeuralNet

import breeze.linalg.{DenseMatrix, DenseVector}
import com.self.core.baseApp.myAPP
import org.apache.spark.{HashPartitioner, Partitioner, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object ART1Net extends myAPP {
  val trainData = Seq(
    new DenseVector[Double](Array(0, 1, 1, 1, 0, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 1).map(_.toDouble)),
    new DenseVector[Double](Array(1, 1, 1, 1, 0, 1, 0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0, 1, 1, 1, 1, 1, 0).map(_.toDouble)),
    new DenseVector[Double](Array(1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0).map(_.toDouble)),
    new DenseVector[Double](Array(1, 1, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 0).map(_.toDouble)),
    new DenseVector[Double](Array(1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 1).map(_.toDouble)),
    new DenseVector[Double](Array(0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 1).map(_.toDouble)),
    new DenseVector[Double](Array(1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1).map(_.toDouble)),
    new DenseVector[Double](Array(1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1).map(_.toDouble)),
    new DenseVector[Double](Array(1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1).map(_.toDouble)),
    new DenseVector[Double](Array(0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1).map(_.toDouble)),
    new DenseVector[Double](Array(1, 1, 1, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 1, 1, 1, 0).map(_.toDouble)),
    new DenseVector[Double](Array(1, 1, 1, 1, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 0).map(_.toDouble))
  )


  override def run(): Unit = {
    println("this is a main file")

    val rho: Double = 0.9
    val numFeatures = 25
    val initialRecNeurons = 1
    val initialWMatrix: DenseMatrix[Double] = DenseMatrix.ones[Double](initialRecNeurons, numFeatures) :* (1.0 / (1 + numFeatures))
    val initialTMatrix: DenseMatrix[Double] = DenseMatrix.ones[Double](numFeatures, initialRecNeurons)

    def train(vec: DenseVector[Double], initialWMatrix: DenseMatrix[Double], initialTMatrix: DenseMatrix[Double])
    : (Int, DenseMatrix[Double], DenseMatrix[Double]) = {
      var wMatrix = initialWMatrix
      var tMatrix = initialTMatrix

      val recVec: DenseVector[Double] = wMatrix * vec // 此时是识别层
      val valueWithIndex: Array[(Double, Int)] = recVec.data.zipWithIndex
      val sum = vec.data.sum

      var neurons = valueWithIndex
      var flag = false // 标识没有找到合适的

      var maxIndex = -1
      var maxTij = DenseVector.zeros[Double](1)
      var similarity = Double.NaN

//      println("-" * 80)
//      println("neurons:", neurons.mkString(", "))
      while (!neurons.isEmpty && !flag) {
        maxIndex = neurons.maxBy(_._1)._2

//        println(s"识别层最大的神经元id为$maxIndex")

        maxTij = tMatrix(::, maxIndex).toDenseVector

        similarity = maxTij dot vec

//        println(s"相似度为${similarity / sum}")
        if (similarity / sum >= rho) {
          flag = true
        } else {
          val drop = neurons.map(_._1).zipWithIndex.maxBy(_._1)._2
          neurons = neurons.slice(0, drop) ++ neurons.slice(drop + 1, neurons.length)
        }
      }


      if (flag) { // 标识找到合适的, 更新合适的权值
//        println("找到合适的")
        val vl = maxTij :* vec
        tMatrix(::, maxIndex) := vl
        wMatrix(maxIndex, ::).inner := vl :* (1 / (1 - 1.0 / numFeatures + similarity))
      } else { // 标识没找到合适的, 新建一个合适的
//        println("没有找到合适的，造一个")
        maxIndex = wMatrix.rows
        tMatrix = DenseMatrix.horzcat(tMatrix, DenseMatrix.ones[Double](numFeatures, 1))
        wMatrix = DenseMatrix.vertcat(wMatrix, DenseMatrix.ones[Double](1, numFeatures) :* (1.0 / (1 + numFeatures)))
      }

//      println("迭代后的t矩阵为：")
//      println(tMatrix)
//      println("迭代后的w矩阵为：")
//      println(wMatrix)
      (maxIndex, wMatrix, tMatrix)
    }

    var wMatrix = initialWMatrix
    var tMatrix = initialTMatrix

//    trainData.foreach {
//      vec =>
//        val (cluster, wM, tM) = train(vec, wMatrix, tMatrix)
//        wMatrix = wM
//        tMatrix = tM
//        println("cluster:", cluster)
//    }


    var i = 1
    trainData.foreach {
      vec =>
        val (cluster, wM, tM) = train(vec, wMatrix, tMatrix)
        wMatrix = wM
        tMatrix = tM
        println(s"第${i}幅图")

        for (i <- 0 until 5) {
          if(i == 2) {
            println(vec.data.slice(i * 5, i * 5 + 5).map(_.toInt).mkString("   ") + "       =====>   " + s"类别为: $cluster")
          } else {
            println(vec.data.slice(i * 5, i * 5 + 5).map(_.toInt).mkString("   "))
          }
        }
        i += 1
        println()

    }







    }
}
