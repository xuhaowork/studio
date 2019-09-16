package com.self.core.VAR.models

import breeze.linalg.{inv, DenseMatrix => BDM}
import scala.collection.mutable.ArrayBuffer

class VAR(val P: Int) extends Serializable {
  def run(ts: BDM[Double]): VARModel = {
    val K = ts.cols
    val P = this.P

    /** 求滞后项 */
    import MatrixLagImplicit.DenseMatrixLag
    val lagData: BDM[Double] = ts.lag(P)

    // 用于求期望的索引，目的是形成k*k的滞后协方差针(k为变量数) => Map[(变量id, 滞后阶数), 在lagData中的列id]
    val index: Map[(Int, Int), Int] = Array.tabulate(K, P + 1)((varId, lagNum) => (varId, lagNum)).flatten.zipWithIndex.toMap


    /**
      * 求[gamma(0), ..., gamma(P)]放入缓存
      */
    val GammaMatrix: Map[Int, BDM[Double]] = Array.tabulate(P + 1)(p => (p, gamma(p, lagData, K, index))).toMap

    /**
      * 求yule-walker方程的协防差阵, 即L_xx, (K * P) * (K * P)维
      * ----
      * 由于breeze矩阵乘法必须匹配, 因此只能通过Array重新构造矩阵
      * 矩阵形式如下
      * gamma(0)(::, 0), gamma(0)(::, 1), .., gamma(0)(::, K-1), gamma(1)(::, 0), .., gamma(1)(::, K-1), .., .., gamma(P-1)(::, K-1)
      * gamma(1)(::, 0), gamma(1)(::, 1), .., gamma(1)(::, K-1), gamma(0)(::, 0), .., gamma(0)(::, K-1), .., .., gamma(0)(::, K-1)
      * ...              ...                  ...                ...
      * gamma(P-1)(::, 0), gamma(P-1)(::, 1), .., gamma(P-1)(::, K-1), gamma(P-2)(::, K-1), .., .., gamma(P-1)(::, K-1)
      */
    val sigma = Array.tabulate(P, P)((i, j) => scala.math.abs(i - j)).flatMap(lagArr => {
      var arr = ArrayBuffer.empty[Double]
      for (k <- 0 until K; eachLag <- lagArr)
        arr ++= GammaMatrix(eachLag)(::, k).toArray
      arr
    }) /* 先形成协防差的滞后阶数，然后将gamma(滞后阶数)每列拼起来，形成一个长列作为协防差阵的data */

    val L_xx: BDM[Double] = new BDM(K * P, K * P, sigma)

    /**
      * 求yule-walker方程的自变量因变量的相关矩阵, 即L_xy, (K * P) * K维
      */
    val covXY = Array.tabulate(K, P)((k, i) => gamma(i + 1, lagData, K, index)(::, k).toArray)
      .flatten.reduceLeft(_ ++ _)
    val L_xy = new BDM(K * P, K, covXY)

    /**
      * 求解方程得到混合系数矩阵, ((K * P)* K维)
      */

    val multiCoefficient = safeInv(L_xx, 0.01) * L_xy

    /**
      * 将混合系数矩阵拆为Array[A1, A2, A3, A4, ..., Ap]
      */
    val coefficient: Array[BDM[Double]] = Array.tabulate(P)(p => {
      val data = Array.range(p * K, K * K * P, K * P)
        .flatMap(start => multiCoefficient.data.slice(start, start + K))
      new BDM(K, K, data)
    })

    new VARModel(ts, K, P, coefficient)
  }


  def safeInv(bm: BDM[Double], alpha: Double = 0.0): BDM[Double] = util.Try(
    inv(bm)).getOrElse(try{
    inv(bm + (BDM.ones[Double](bm.rows, bm.cols) :* alpha))
  }catch{
    case _: Exception => throw new Exception("模型再加入正则项后仍无法识别，请检查您输入的数据是否存在以下情况：" +
      "1）数据数目少于变量数和滞后数的乘积；2）数据严重多重共线性问题，请检查是否有几列数据相同或者多列间可以相互线性表示。")
  })



  /**
    * 求y_t(y_0_t, y_1_t, ..., y_k-1_t)滞后n阶的协方差阵，其中第(i, j)个元素为sum_t [y_i_t, y_j_t]
    *
    * @param lagNum 滞后阶数
    * @return
    */
  def gamma(lagNum: Int, lagData: BDM[Double], K: Int, index: Map[(Int, Int), Int])
  : BDM[Double] = {
    /** 先求协方差矩阵的上三角矩阵，将其放入缓存 */
    val expectMap = Array.tabulate(K)(i => Array.tabulate(K - i)(j => {
      val realJ = j + i
      // 求第i个变量和第realJ个变量滞后lag阶的协方差
      val ts_i = lagData(::, index(i, 0))
      val ts_j = lagData(::, index(realJ, lagNum))
      ((i, realJ), ts_i dot ts_j)
    })).flatten.toMap
    /**
      * 再将上三角阵变为对称的协防差阵
      */
    val data = Array.tabulate(K, K)((i, j) => if (i <= j) expectMap(i, j) else expectMap(j, i)).flatten
    new BDM[Double](K, K, data)
  }


}

class VARModel(val rawData: BDM[Double], val K: Int, val P: Int, val coefficient: Array[BDM[Double]]) extends Serializable {
  def fit: BDM[Double] = {
    var i = 0
    val buffer = ArrayBuffer.empty[BDM[Double]]
    while(i < rawData.rows - P) {
      val fitMatrix = coefficient.zipWithIndex.map {
        case (coef, p) => {
          val newBV = rawData(i + P - p, ::).inner.toDenseMatrix
          val multiV = coef * newBV.t
          multiV.t
        }
      }.reduce(_ + _)
      buffer += fitMatrix
      i += 1
    }
    BDM.vertcat(buffer:_*)
  }


  def predict(pSteps: Int): BDM[Double] = {
    var bv = rawData
    require(rawData.rows >= coefficient.length, "markov过程需要输入的时间数不能少于模型要求的滞后阶数")
    require(rawData.cols == coefficient.head.rows && coefficient.head.rows == K, "模型系数以及输入的数据纬度和K需要一致")
    var bvLength = rawData.rows

    var i = 0
    while (i < pSteps) {
      val fitMatrix = coefficient.zipWithIndex.map {
        case (coef, p) => {
          val newBV = bv(bvLength - p - 1, ::).inner.toDenseMatrix
          val multiV = coef * newBV.t
          multiV.t
        }
      }.reduce(_ + _)
      bv = BDM.vertcat(bv, fitMatrix)
      i += 1
      bvLength += 1
    }
    bv
  }

}

