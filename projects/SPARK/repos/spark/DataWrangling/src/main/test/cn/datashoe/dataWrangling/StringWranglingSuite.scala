package cn.datashoe.dataWrangling

import org.scalatest.FunSuite

class StringWranglingSuite extends FunSuite {
  test("float格式化") {
    println(StringWrangling.floatFormat(15, 3)(0.777888999000))
    println(StringWrangling.floatFormat(4, 2)(0.777888999000 * 100))
    println(StringWrangling.floatFormat(4, 1)(0.777888999000))

  }

}
