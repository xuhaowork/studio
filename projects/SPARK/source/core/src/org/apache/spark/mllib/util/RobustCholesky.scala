package org.apache.spark.mllib.util

import breeze.generic.UFunc
import breeze.linalg.{DenseMatrix, Matrix, MatrixEmptyException, MatrixNotSquareException, MatrixNotSymmetricException, NotConvergedException, lowerTriangular, max}
import breeze.numerics.abs
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW


/**
  * 矩阵的Cholesky分解，由于breeze.linalg中的cholesky分解经常碰到对称阵识别为非对称阵的问题，
  * 这里重写一个脚本避免该问题。
  * ----
  * 注意：这里构建了一个非目录结构的package，breeze.linalg
  */
object RobustCholesky extends UFunc {

  implicit object ImplCholesky_DM extends Impl[DenseMatrix[Double], DenseMatrix[Double]] {
    def apply(X: DenseMatrix[Double]): DenseMatrix[Double] = {
      if (X.cols == 0 || X.rows == 0)
        throw new MatrixEmptyException



      // As LAPACK doesn't check if the given matrix is in fact symmetric,
      // we have to do it here (or get rid of this time-waster as long as
      // the caller of this function is clearly aware that only the lower
      // triangular portion of the given matrix is used and there is no
      // check for symmetry).
      requireSymmetricRobust(X)

      // Copy the lower triangular part of X. LAPACK will store the result in A
      val A: DenseMatrix[Double] = lowerTriangular(X)

      val N = X.rows
      val info = new intW(0)
      lapack.dpotrf(
        "L" /* lower triangular */ ,
        N /* number of rows */ ,
        A.data,
        scala.math.max(1, N) /* LDA */ ,
        info
      )
      // A value of info.`val` < 0 would tell us that the i-th argument
      // of the call to dpotrf was erroneous (where i == |info.`val`|).
      assert(info.`val` >= 0)

      if (info.`val` > 0)
        throw new NotConvergedException(NotConvergedException.Iterations)

      A
    }


    def requireSymmetricRobust(mat: Matrix[Double], tol: Double = 1e-7): Unit = {
      if (mat.rows != mat.cols)
        throw new MatrixNotSquareException

      for (i <- 0 until mat.rows; j <- 0 until i)
        if (abs(mat(i, j) - mat(j, i)) > max(abs(mat(i, j)), abs(mat(j, i)), 1.0) * tol)
          throw new MatrixNotSymmetricException
    }

  }

}


