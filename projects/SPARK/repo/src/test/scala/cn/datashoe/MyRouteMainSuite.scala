package cn.datashoe


import org.scalatest.{FunSuite, Suite}

class MyRouteMainSuite extends FunSuite {
  test("运行") {
    object TestMyRouteMain extends MyRouteMain {
      override def run(): Unit = {
        println("This is a file testing the class MyRouteMain.")
      }
    }
    TestMyRouteMain.run()

  }

}


object TestMyRouteMain extends MyRouteMain {
  override def run(): Unit = {
    println("This is a file testing the class MyRouteMain.")
    val rate = 0.99752
    println("%2.2f".format(rate * 100))

    println(System.getenv("MY_WORKPLACE_SPARK"))

  }
}
