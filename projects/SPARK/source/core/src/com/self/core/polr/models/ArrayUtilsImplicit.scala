package com.self.core.polr.models

/**
  * 有序回归用到的一个隐式转换类
  */

/**
  * editor: xuhao
  * date: 2018-05-05 10:30:00
  */
object ArrayUtilsImplicit {

  implicit class ArrayCumsum(values: Array[Double]) {
    def cumsum: Array[Double] = values
      .foldLeft(Array.empty[Double])((arr, v) => arr :+ (if (arr.isEmpty) v else arr.last + v))
  }

}
