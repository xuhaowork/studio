package com.self.core.probitRegression

import java.util.Random

import breeze.linalg.{MatrixEmptyException, MatrixNotSymmetricException, cholesky, DenseMatrix => BDM, DenseVector => BDV}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.util.VectorBLAS._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * probit回归的测试数据
  */

/**
  * editor: xuhao
  * date: 2018-05-15 10:30:00
  */

object TestData {
  /**
    * 模拟测试数据的类
    *
    * @param num       数目
    * @param seed      随机数种子
    * @param weight    系数
    * @param intercept 截距项
    * @return 以Array存储的数据
    */
  def simulate(num: Int,
               seed: Long,
               weight: Vector,
               intercept: Double = 0.0): Array[Array[Double]] = {
    val rd = new java.util.Random(seed)
    import breeze.stats.distributions.Gaussian
    val gaussian = new Gaussian(0.0, 1.0)

    Array.fill(num)(new DenseVector(Array(rd.nextGaussian()*2 + 1.0, rd.nextGaussian())))
      .map(v => {
        val label = if (rd.nextDouble() <= gaussian.cdf(dot(v, weight) + intercept)) 1.0 else 0.0
        v.values :+ label
      })
  }


  def multiNorm(num: Int, seed: Long, mean: BDV[Double], sigma: BDM[Double], rd: Random)
  : Array[BDV[Double]] = {
    require(mean.length == sigma.rows && mean.length == sigma.cols, "多元正态分布的均值和方差的维度应该一致")
    val sqrtSigma = try {
      cholesky(sigma)
    } catch {
      case _: MatrixNotSymmetricException => throw new Exception("您输入的不是对阵矩阵")
      case _: MatrixEmptyException => throw new Exception("您输入的是空矩阵")
      case _: Exception => throw new Exception("在进行多元正态分布的随机中，求cholesky分解有误")
    }

    Array.tabulate(num, mean.length)((_, _) => rd.nextGaussian()).map(arr => {
      val v = new BDV[Double](arr)
      sqrtSigma * v
    })
  }


  def simulateMulti(sc: SparkContext, sqlc: SQLContext): DataFrame = {
    //    val p = 2 // 2维
    //    val J = 3 // 3分类

    /**
      * 进来的数据结构
      */
    import org.apache.spark.mllib.linalg.DenseVector
    import org.apache.spark.mllib.util.VectorBLAS.dot
    val rd = new java.util.Random(123L)
    val arr = Array.tabulate(5000, 2)((_, _) => rd.nextGaussian())

    val beta0 = new DenseVector(Array(0.9, 0.3))
    val beta1 = new DenseVector(Array(-0.45, -1.3))
    val beta2 = new DenseVector(Array(0.4, 0.6))
    val beta = Array(beta0, beta1, beta2)

    val data = arr.map(ar => {
      val v = new DenseVector(ar)

      val epsilon = multiNorm(
        1,
        123L,
        new BDV(Array(0.0, 0.0, 0.0)),
        new BDM[Double](3, 3, Array(1.0, -0.5, -0.0, -0.5, 1.0, 0.0, 0.0, 0.0, 1.0)),
        rd).head
      val implicitVariables = beta.map(ceof => dot(ceof, v))
      val maxIndex = implicitVariables
        .zipWithIndex
        .map {
          case (value, index) => (value + epsilon(index), index)
        }.maxBy(_._1)._2
      ar :+ maxIndex.toDouble
    })

    val rdd = sc.parallelize(data)

    sqlc.createDataFrame(rdd.map(Row.fromSeq(_)), StructType(Array(
      StructField("x1", DoubleType),
      StructField("x2", DoubleType),
      StructField("y", DoubleType)
    )))

  }

}
