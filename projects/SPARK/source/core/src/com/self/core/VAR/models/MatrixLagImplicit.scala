package com.self.core.VAR.models

import breeze.linalg.{DenseMatrix => BDM}

/**
  * 将用于VAR模型的breeze.DenseMatrix生成延迟矩阵
  * ----
  * 具体思路：
  * 1)将DenseMatrix[Double]转为按列存储的Array[ Array[Double] ]形式
  * 2)将Array[ Array[Double] ]中内部的每个列数据即Array[Double]变为滞后0到maxLag阶的数据，由Array[ Array[Double] ]形式存储
  * 此时的数据变为Array[ Array[ Array[Double] ] ]形式，即`观测数` * `滞后数` * `变量数` 维矩阵.
  * 3)再将Array[ Array[ Array[Double] ] ]flatten为Array[ Array[Double] ]，其中的Array[Double]变为了`变量数` * `滞后数`个
  */

/**
  * 请注意：
  * 1）默认时间是随着矩阵行数升序排列的, 这很重要
  * 2）默认数据是按列放在Array中的
  * 3）默认数据的数值是Double类型
  */

object MatrixLagImplicit {


  /**
    * Example input TimeSeries:
    *   time   a   b
    *   4 pm   1   6
    *   5 pm   2   7
    *   6 pm   3   8
    *   7 pm   4   9
    *   8 pm   5   10
    *
    * With maxLag 2, includeOriginals = true and TimeSeries.laggedStringKey, we would get:
    *   time   a   lag1(a)   lag2(a)  b   lag1(b)  lag2(b)
    *   6 pm   3   2         1         8   7         6
    *   7 pm   4   3         2         9   8         7
    *   8 pm   5   4         3         10  9         8
    */

  /** 单个向量的延迟矩阵 -- x => x_0, x_1, x_2, ... */
  implicit class ArrayInverseSliding(val values: Array[Double]){
    def lag(step: Int, includeOriginal: Boolean = true): Array[Array[Double]] =
      if(includeOriginal) {
        Array.tabulate(step + 1)(lagNum => Array.range(step - lagNum, values.length - lagNum).map(values.apply))
      } else {
        Array.tabulate(step)(lagNum => Array.range(step - lagNum - 1, values.length - lagNum - 1).map(values.apply))
      }
  }

  /** ArrayArray形式存储二维矩阵的延迟矩阵 --(x, y, ..) => ((x_0, x_1, x_2, ...), (y_0, y_1, y_2, ...), ...)  */
  private def lagMat(x: Array[Array[Double]], maxLag: Int, includeOriginal: Boolean)
  : Array[Array[Double]] = {
    x.flatMap(feature => feature.lag(maxLag, includeOriginal))
  }



  implicit class DenseMatrixLag(val bm: BDM[Double]){
    import MatrixNAFillImplicit._
    def lag(maxLag: Int = 1, includeOriginal: Boolean = true): BDM[Double] = {
      val arrayArray = bm.toArrayArray
      lagMat(arrayArray, maxLag, includeOriginal).toDenseMatrix()
    }
  }


  implicit class ArrayArrayLg(val arr: Array[Array[Double]]){
    def lag(maxLag: Int = 1, includeOriginal: Boolean = true): Array[Array[Double]] =
      lagMat(arr, maxLag, includeOriginal)
  }

}
