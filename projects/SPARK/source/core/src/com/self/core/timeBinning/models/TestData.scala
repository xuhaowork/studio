package com.self.core.timeBinning.models

object TestData {
  val testData1 = Array(
    "2017/02/10 00:00:00", 1, 2,
    "2017/02/08 01:00:01", 1, 2,
    "2017/3/1 04:00:02", 1, 2,
    "2017/4/10 00:15:03", 1, 2,
    null, 1, 2,
    "2017/04/20 07:20:05", 1, 2,
    "2017/04/30 08:01:06", 1, 2,
    "2017/04/30 09:11:06", 1, 2,
    "2017/04/30 16:01:06", 1, 2,
    "2017/06/10 13:01:06", 1, 2,
    "2017/08/10 00:00:00", 1, 2,
    "2017/08/18 01:00:01", 1, 2,
    "2017/11/1 04:00:02", 1, 2,
    "2017/12/31 00:15:03", 1, 2,
    "2017/04/10 06:20:04", 1, 2,
    "2018/01/1 07:20:05", 1, 2,
    "2018/02/19 13:01:06", 1, 2,
    "2018/03/2 13:01:06", 1, 2,
    "2018/03/9 13:01:06", 1, 2,
    "2018/04/1 13:01:06", 1, 2)


  implicit class ArrayToMatrix[T](val values: Array[T]){
    def toMatrixArrayByCol(rows: Int, cols: Int) = {
      require(values.lengthCompare(rows * cols) == 0)
      Array.range(0, cols).map(col => values.slice(col * rows, (col + 1) * rows))
    }


    def toMatrixArrayByRow(rows: Int, cols: Int) = {
      require(values.lengthCompare(rows * cols) == 0)
      Array.range(0, rows).map(row => values.slice(row * cols, (row + 1) * cols))
    }

  }


}
