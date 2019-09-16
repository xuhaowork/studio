package cn.datashoe.base

import org.scalatest.FunSuite

class APPSuite extends FunSuite {
  test("APP") {
    object TestAPP extends APP {
      override def run(): Unit = {
        println("This is a test file.")
      }
    }

    TestAPP.run()
  }


}
