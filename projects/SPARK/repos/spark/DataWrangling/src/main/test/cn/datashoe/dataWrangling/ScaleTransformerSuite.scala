package cn.datashoe.dataWrangling

import org.scalatest.FunSuite

class ScaleTransformerSuite extends FunSuite {
  test("尺度转换中对Infinity的支持") {
    println(ScaleTransformer.log(0.0))
    println(Double.NegativeInfinity)
    println(ScaleTransformer.log(0.0) == Double.NegativeInfinity)
    println(Double.NegativeInfinity <= -1554876878.0)
    println(Double.NegativeInfinity + Double.NegativeInfinity)

  }




}
