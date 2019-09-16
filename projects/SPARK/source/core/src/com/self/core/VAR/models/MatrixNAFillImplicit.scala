package com.self.core.VAR.models

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Vector => BV}
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator


/**
  * 按行或列对breeze的DenseVector、DenseMatrix进行缺失值填充
  * ----
  * 1）对breeze的DenseVector的缺失值进行补全
  * 2）对breeze的DenseMatrix类型的元素进行缺失值补全（稀疏矩阵当然要变为DenseMatrix才能补全）
  */
object MatrixNAFillImplicit extends Serializable {

  /**
    * @param values 为二维向量，默认从外道第一维为列，第二维为行
    *            即："Array[ Array[Double] ]"里面的Array[Double]代表列, storedByCols为true即数据按列储存
    */
  implicit class ArrayToDenseMatrix(val values: Array[Array[Double]]){
    def toDenseMatrix(storedByCols: Boolean = true): BDM[Double] = {
      val cols = values.length
      val rows = values.head.length
      if(storedByCols)
        new BDM(rows, cols, values.flatten)
      else
        new BDM(cols, rows, values.flatten, 0, rows, true)
    }

    def fillByCols(fillMethod: String, storedByCols : Boolean = true): Array[Array[Double]] = {
      val arrayArray = if(storedByCols) values else values.transpose
      arrayArray.map(fillArray(_, fillMethod))
    }

    def fillByRows(fillMethod: String, storedByCols : Boolean = true): Array[Array[Double]] = {
      val arrayArray = if(! storedByCols) values else values.transpose
      arrayArray.map(fillArray(_, fillMethod))
    }


  }

  implicit class DenseMatrixToArrayArray(val bm: BDM[Double]){
    def toArrayArray: Array[Array[Double]] =
      if(bm.isTranspose)
        Array.range(0, bm.size, bm.cols)
          .map(start => bm.data.slice(start, start + bm.cols)).transpose
      else
        Array.range(0, bm.size, bm.rows)
          .map(start => bm.data.slice(start, start + bm.rows))

    def fillByCols(fillMethod: String): BDM[Double] =
      bm.toArrayArray.fillByCols(fillMethod).toDenseMatrix()

    def fillByRows(fillMethod: String): BDM[Double] =
      bm.toArrayArray.fillByRows(fillMethod).toDenseMatrix()

  }


  private def fillArray(ts: Array[Double], fillMethod: String): Array[Double] = {
    fillMethod match {
      case "linear" => fillLinear(ts)
      case "nearest" => fillNearest(ts)
      case "next" => fillNext(ts)
      case "previous" => fillPrevious(ts)
      case "spline" => fillSpline(ts)
      case "zero" => fillValue(ts, 0)
      case _ => throw new UnsupportedOperationException()
    }
  }


  /**
    * Replace all NaNs with a specific value
    */
  private def fillValue(values: Array[Double], filler: Double): Array[Double] = {
    fillValue(new BDV[Double](values), filler).toArray
  }

  /**
    * Replace all NaNs with a specific value
    */
  private def fillValue(values: BV[Double], filler: Double): BDV[Double] = {
    val result = values.copy.toArray
    var i = 0
    while (i < result.size) {
      if (result(i).isNaN) result(i) = filler
      i += 1
    }
    new BDV[Double](result)
  }

  private def fillNearest(values: Array[Double]): Array[Double] = {
    fillNearest(new BDV[Double](values)).toArray
  }

  private def fillNearest(values: BV[Double]): BDV[Double] = {
    val result = values.copy.toArray
    var lastExisting = -1
    var nextExisting = -1
    var i = 1
    while (i < result.length) {
      if (result(i).isNaN) {
        if (nextExisting < i) {
          nextExisting = i + 1
          while (nextExisting < result.length && result(nextExisting).isNaN) {
            nextExisting += 1
          }
        }

        if (lastExisting < 0 && nextExisting >= result.length) {
          throw new IllegalArgumentException("Input is all NaNs!")
        } else if (nextExisting >= result.length || // TODO: check this
          (lastExisting >= 0 && i - lastExisting < nextExisting - i)) {
          result(i) = result(lastExisting)
        } else {
          result(i) = result(nextExisting)
        }
      } else {
        lastExisting = i
      }
      i += 1
    }
    new BDV[Double](result)
  }

  private def fillPrevious(values: Array[Double]): Array[Double] = {
    fillPrevious(new BDV[Double](values)).toArray
  }

  /**
    * fills in NaN with the previously available not NaN, scanning from left to right.
    * 1 NaN NaN 2 Nan -> 1 1 1 2 2
    */
  private def fillPrevious(values: BV[Double]): BDV[Double] = {
    val result = values.copy.toArray
    var filler = Double.NaN // initial value, maintains invariant
    var i = 0
    while (i < result.length) {
      filler = if (result(i).isNaN) filler else result(i)
      result(i) = filler
      i += 1
    }
    new BDV[Double](result)
  }

  private def fillNext(values: Array[Double]): Array[Double] = {
    fillNext(new BDV[Double](values)).toArray
  }

  /**
    * fills in NaN with the next available not NaN, scanning from right to left.
    * 1 NaN NaN 2 Nan -> 1 2 2 2 NaN
    */
  private def fillNext(values: BV[Double]): BDV[Double] = {
    val result = values.copy.toArray
    var filler = Double.NaN // initial value, maintains invariant
    var i = result.length - 1
    while (i >= 0) {
      filler = if (result(i).isNaN) filler else result(i)
      result(i) = filler
      i -= 1
    }
    new BDV[Double](result)
  }

  private def fillWithDefault(values: Array[Double], filler: Double): Array[Double] = {
    fillWithDefault(new BDV[Double](values), filler).toArray
  }

  /**
    * fills in NaN with a default value
    */
  private def fillWithDefault(values: BV[Double], filler: Double): BDV[Double] = {
    val result = values.copy.toArray
    var i = 0
    while (i < result.length) {
      result(i) = if (result(i).isNaN) filler else result(i)
      i += 1
    }
    new BDV[Double](result)
  }

  private def fillLinear(values: Array[Double]): Array[Double] = {
    fillLinear(new BDV[Double](values)).toArray
  }

  private def fillLinear(values: BV[Double]): BDV[Double] = {
    val result = values.copy.toArray
    var i = 1
    while (i <= result.length - 1) {
      val rangeStart = i
      while (i <= result.length - 1 && result(i).isNaN) {
        i += 1
      }
      val before = result(rangeStart - 1)
      if(i == result.length){
        i = result.length - 1
        if(before.isNaN){
          throw new Exception("All NAs！")
        }else{
          result(i) = before
        }
      } // 如果末位为NaN则以最后一个非NaN值填充
      if(before.isNaN){
        result(rangeStart - 1) = result(i)
      }

      val after =  result(i)
      if (i != rangeStart && !before.isNaN) {
        val increment = (after - before) / (i - (rangeStart - 1))
        for (j <- rangeStart to i) {
          result(j) = result(j - 1) + increment
        }
      }
      i += 1
    }
    new BDV[Double](result)
  }

  private def fillSpline(values: Array[Double]): Array[Double] = {
    fillSpline(new BDV[Double](values)).toArray
  }

  /**
    * Fill in NaN values using a natural cubic spline.
    * @param values Vector to interpolate
    * @return Interpolated vector
    */
  private def fillSpline(values: BV[Double]): BDV[Double] = {
    val result = values.copy.toArray
    val interp = new SplineInterpolator()
    val knotsAndValues = values.toArray.zipWithIndex.filter(!_._1.isNaN)
    // Note that the type of unzip is missed up in scala 10.4 as per
    // https://issues.scala-lang.org/browse/SI-8081
    // given that this project is using scala 10.4, we cannot use unzip, so unpack manually
    val knotsX = knotsAndValues.map(_._2.toDouble)
    val knotsY = knotsAndValues.map(_._1)
    val filler = interp.interpolate(knotsX, knotsY)

    // values that we can interpolate between, others need to be filled w/ other function
    var i = knotsX(0).toInt
    val end = knotsX.last.toInt

    while (i < end) {
      result(i) = filler.value(i.toDouble)
      i += 1
    }
    new BDV[Double](result)
  }


}
