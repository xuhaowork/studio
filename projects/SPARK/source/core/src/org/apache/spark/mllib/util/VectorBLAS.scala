package org.apache.spark.mllib.util

import org.apache.spark.mllib.linalg.{BLAS, Vector}

/**
  * editor: xuhao
  * date: 2018-03-26 08:30:00
  */

/**
  * 提供一些将mllib.BLAS中的算法对外开放的一个接口
  */

object VectorBLAS {
  /**
    * y += a * x
    */
  def axpy(a: Double, x: Vector, y: Vector) = BLAS.axpy(a: Double, x: Vector, y: Vector)

  /**
    * x = a * x
    */
  def scal(a: Double, x: Vector) = BLAS.scal(a: Double, x: Vector)

  /**
    * dot(x, y)
    */
  def dot(x: Vector, y: Vector): Double = BLAS.dot(x, y)


}
