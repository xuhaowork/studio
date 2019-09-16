package org.apache.spark.mllib.util

import java.util.Random

import breeze.linalg.{MatrixEmptyException, MatrixNotSymmetricException, cholesky, DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.Gaussian
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.BLAS.axpy
import org.apache.spark.mllib.util.RobustCholesky

/**
  * probit求多分类是需要的工具文件
  * ----
  * 具体包括一下接口：
  *
  * @define multiNorm                多元正态的模拟
  * @define VectorToTriangleMatrix   breeze向量转下三角矩阵的一个隐式转换
  * @define minusElementaryMatrix    生成初等变换方阵的函数
  * @define getNegativeLogLikelihood 获得负的对数似然函数
  * @define getGradient              获得梯度
  *                                  ----
  *                                  另外还有上述接口调用的函数：
  * @define smoothRecurse            实现了GHK的smooth recurse Markov-MonteCarlo求积分
  * @define recurse                  被smoothRecurse调用
  */

/**
  * editor: xuhao
  * date: 2018-05-15 10:30:00
  */
object ToolsForMNP {

  // 模拟多元正态分布
  def multiNorm(num: Int, rd: Random, mean: BDV[Double], sigma: BDM[Double])
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
      sqrtSigma * v + mean
    })
  }


  implicit class VectorToTriangleMatrix(val values: BDV[Double]) {
    def toTriangle: BDM[Double] = {
      val rootForVectorLength = (-1 + scala.math.sqrt(values.length * 8 + 1)) / 2

      require(rootForVectorLength.toInt == rootForVectorLength, "输入的向量长度不能够恰好构成一个下三角矩阵")
      val dim = rootForVectorLength.toInt
      var start = 0
      val data = for (i <- (1 to dim).reverse) yield {
        val arr = values.data.slice(start, start + i)
        start += i
        Array.fill[Double](dim - i)(0.0) ++ arr
      }
      new BDM(dim, dim, data.toArray.flatten)
    }

  }


  def minusElementaryMatrix(dim: Int, q: Int): BDM[Double] = {
    require(q < dim && q >= 0, "维数不一致, q不符合矩阵维数条件")
    val buffer = -BDM.eye[Double](dim)
    buffer(::, q) := 1.0
    buffer
  }


  def getNegativeLogLikelihood(data: linalg.Vector,
                               label: Double,
                               weights: linalg.Vector,
                               num: Int,
                               R: Int = 100): Double = {
    require(weights.size == data.size * num + num * (num - 1) / 2, "参数数目不一致") // weight需要为 num * p + num * (num - 1) / 2维
    import com.self.core.polr.models.VectorImplicit.VectorLastImplicit
    val halfL = (1.0 +: Array.fill[Double](num - 1)(0.0)) ++ weights.last(num * (num - 1) / 2) // 取出rho,避免无法识别，需要L11 = 1, L_{j, 1} = 0

    val L: BDM[Double] = new BDV(halfL).toTriangle // 生成下三角矩阵 num * num维
    require(label < num && label >= 0, "label的类型应该标注为0.0至num - 1") // label 应该在0.0到num之间(左闭右开)

    val Aq = minusElementaryMatrix(num, label.toInt) // num * num维
    val P_triangle: BDM[Double] = RobustCholesky(Aq * L * L.t * Aq.t) // 得到Aq对应的协防差阵cholesky变换 num * num维
    val R = 100
    val seed = 123L


    val betaArray = weights.first(num * data.size)
    val beta = new BDM[Double](num, data.size, betaArray, 0, data.size, true) // num * p维
    val a = Aq * beta * {new BDV[Double](data.toDense.values)}

    -scala.math.log(ToolsForMNP.smoothRecurse(R, P_triangle, a, seed) + 1E-7) // 通过MC(GHK-smooth recurse方法)求近似似然
  }

  def getGradient(data: linalg.Vector,
                  label: Double,
                  weights: linalg.Vector,
                  cumGradient: linalg.Vector,
                  num: Int,
                  diff: Double = 1E-5
                 ): Double = {
    val oldLoss: Double = getNegativeLogLikelihood(data: linalg.Vector,
      label: Double,
      weights: linalg.Vector,
      num: Int)
    val gradient = for (i <- 0 until weights.size)
      yield {
        val denseWeights = weights.toDense
        denseWeights.values(i) = denseWeights.values(i) + scala.math.min(denseWeights.values(i), 1.0) * diff
        val newLoss = getNegativeLogLikelihood(data: linalg.Vector,
          label: Double,
          denseWeights: linalg.Vector,
          num: Int)
        (newLoss - oldLoss) / diff
      }
    axpy(1.0, new linalg.DenseVector(gradient.toArray), cumGradient)
    oldLoss
  }


  private def smoothRecurse(R: Int, P_triangle: BDM[Double], a: BDV[Double], seed: Long): Double = {
    //    val sampleSeed = new java.util.Random(seed).nextLong()
    val rd: Random = new java.util.Random(123L)
    var sum = 0.0
    var count = 0
    while (count < R) {
      rd.nextDouble()
      val result = recurse(3, P_triangle, a, rd)._2
      sum += result
      count += 1
    }
    sum / R
  }


  private def recurse(i: Int, P_triangle: BDM[Double], a: BDV[Double], rd: Random): (Array[Double], Double) = {
    if (i == 0)
      (Array.empty[Double], 1.0)
    else {
      require(i <= P_triangle.cols, "i不能超过矩阵的列数")
      val tup = recurse(i - 1, P_triangle, a, rd: Random) // 递归
      val epsilon: Array[Double] = tup._1
      val QLikelihood: Double = tup._2

      val f = P_triangle(i - 1, 0 until i)
      val minus = f.inner.toArray.zip(epsilon).map(tup => tup._1 * tup._2).sum
      val B_i = a(i - 1) - minus
      val gaussian = new Gaussian(0.0, 1.0)
      val Q_i = gaussian.cdf(B_i)
      val nextD = rd.nextDouble()

      val epsilon_i = gaussian.icdf(Q_i * nextD)
      (epsilon :+ epsilon_i, QLikelihood * Q_i)
    }
  }

}
