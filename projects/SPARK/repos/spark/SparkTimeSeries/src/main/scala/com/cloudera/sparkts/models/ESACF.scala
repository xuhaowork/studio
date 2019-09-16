package com.cloudera.sparkts.models

import scala.collection.mutable

/**
  * ARIMA定阶所用的ESACF
  * ----
  * 参考文献:
  * Tsay, R. and Tiao, G. (1984).
  * "Consistent Estimates of Autoregressive Parameters and Extended Sample Autocorrelation Function for Stationary and
  * Nonstationary ARMA Models." Journal of the American Statistical Association, 79 (385), pp. 84-96.
  */
object ESACF extends Serializable {

  /** Array[Double]和Array[ Array[Double] ]的隐式转换类 */
  implicit class VectorImpl(val ts: Array[Double]) {
    def lag(step: Int): Array[Double] = Array.fill(step)(Double.NaN) ++ ts.dropRight(step)

    def -(other: Array[Double]): Array[Double] = ts.zip(other).map { case (a, b) => a - b }

    def *(other: Array[Double]): Double = {
      require(ts.length == other.length, s"向量长度不一致不能相乘, ${ts.length}, ${other.length}" +
        s"${ts.mkString(",")}, ${other.mkString(",")}")
      var mul = 0.0
      for (i <- ts.indices) {
        mul += ts(i) * other(i)
      }
      mul
    }

    def *(multiplier: Double): Array[Double] = ts.map(_ * multiplier)


    def ::(other: Array[Double]): Array[Array[Double]] = {
      Array(ts, other)
    }

    def ::(other: Array[Array[Double]]): Array[Array[Double]] = {
      ts +: other
    }

  }

  implicit class MatrixImpl(matrix: Array[Array[Double]]) {
    def ::(vector: Array[Double]): Array[Array[Double]] = {
      matrix :+ vector
    }
  }


  /**
    * 转为上三角矩阵
    *
    * @param m 矩阵 Array(列) 按列存储
    */
  def reupm(m: Array[Array[Double]], nRows: Int, nCols: Int): Array[Array[Double]] =
    Array.tabulate(nCols - 1) {
      i =>
        val work: Array[Double] = -1.0 +: m(i).dropRight(1)
        val tmp = m(i + 1) - work * (m(i + 1)(i + 1) / m(i)(i))
        tmp(i + 1) = 0.0
        tmp
    }

  /**
    * 样本自相关函数
    *
    * @param ts       序列
    * @param maxLag   最大的计算阶数
    * @param dropLag0 是否包括序列本身(也就是滞后0阶)
    * @return
    */
  def acf(ts: Array[Double], maxLag: Int, dropLag0: Boolean = false): Array[Double] = {
    val tsMean = ts.sum / ts.length
    val standardTs = ts.map(_ - tsMean)
    val std = standardTs.map { d => d * d }.sum

    val res = Array.range(1, maxLag + 1).map {
      step =>
        if (std == 0.0)
          1.0
        else
          (standardTs.drop(step) * standardTs.dropRight(step)) / std
    }
    if (dropLag0)
      res
    else {
      1.0 +: res
    }
  }

  /**
    * 计算ESACF函数
    *
    * @param m     中间结果矩阵
    * @param cov   自相关向量（经过变换）
    * @param nar   自相关阶数
    * @param nCol  矩阵列数
    * @param count 计数项
    * @param nCov  自相关向量长度(本身长度 --作为一个位置标记)
    * @param ts    时间序列
    * @param zm    时间序列的滞后矩阵
    * @return ESACF矩阵
    */
  def ceascf(
              m: Array[Array[Double]],
              cov: Array[Double],
              nar: Int,
              nCol: Double,
              count: Int,
              nCov: Int,
              ts: Array[Double],
              zm: Array[Array[Double]]
            ): Array[Double] = {
    val res = new Array[Double](nar + 1)
    res(0) = cov(nCov + count - 1)
    var i = 1

    while (i <= nar) {
      val tmp = (ts.drop(i) +: zm.take(i).map(_.drop(i))).transpose
      val v = -1.0 +: m(i - 1).take(i)
      val tmpTs = tmp.map { arr => arr * v }
      res(i) = acf(tmpTs, count)(count)
      i += 1
    }

    res
  }

  def diff(ts: Array[Double]): Array[Double] = {
    require(ts.length >= 2, "序列长度小于2差分没有意义")
    val res = mutable.ArrayBuilder.make[Double]()
    var i = 0
    var tmpValue = 0.0
    while (i < ts.length - 1) {
      if(i > 0)
        res += tmpValue
      tmpValue = ts(i + 1) - ts(i)
      i += 1
    }
    res += tmpValue
    res.result()
  }



  /**
    * 计算ESACF
    *
    * @param data  数据
    * @param arMax 最大自回归阶数
    * @param maMax 最大移动平均阶数
    * @param dStep 差分阶数
    * @return
    */
  def esacf(data: Seq[Double], arMax: Int = 7, maMax: Int = 13, dStep: Int = 0): Array[Array[String]] = {
    var tmpTs = data.toArray
    Range(0, dStep).foreach {
      _ =>
        tmpTs = diff(tmpTs)
    }
    val tsMean = tmpTs.sum / tmpTs.length

    require(data.length - dStep > arMax + maMax + 3, "序列长度需要大于自回归阶数、移动平均阶数、差分阶数加3的和")

    val nar = arMax
    val nma = maMax + 1

    var ncov = nar + nma + 2
    val nrow = nar + nma + 1
    var ncol = nrow - 1

    val ts = tmpTs.map { d => d - tsMean }

    val zm = Array.range(1, nar + 1).map {
      step =>
        ts.lag(step)
    }


    val cov1 = acf(ts, ncov)
    val cov = cov1.drop(1).reverse ++ cov1
    ncov = ncov + 1


    import org.apache.spark.mllib.linalg.DenseVector
    val vector = new DenseVector(ts)

    val m1 = Array.range(1, ncol + 1).map {
      arStep =>
        Autoregression.fitModel(vector, arStep, noIntercept = true).coefficients ++ Array.fill(nrow - arStep)(0.0)
    }

    var i = 1
    var tmpM = m1

    val values = new Array[Array[Double]](nma)
    while (i <= nma) {
      val m2 = reupm(tmpM, nrow, ncol)
      ncol -= 1
      values(i - 1) = ceascf(m2, cov, nar, ncol, i,
        ncov, ts, zm)
      tmpM = m2
      i += 1
    }

    val work = Array.tabulate(nar + 1)(i => ts.length - i) // 要求length大于nar + 1

    var colFlag = 0
    val result = values.map {
      arr =>
        colFlag += 1
        arr.zip(work).map {
          case (value, bound) =>
            if (scala.math.abs(value) > 2.0 / scala.math.sqrt(bound - colFlag)) "X" else "O"
        }
    }


    val uu = result.transpose
    val mm = uu.zipWithIndex.map {
      case (arr, index) =>
        index.toString +: arr
    }

    mm
  }

}
