package cn.datashoe

/**
  * The Main Class for all run object in Project
  */
abstract class MyRouteMain {
  def run(): Unit

  def main(args: Array[String]): Unit = {
    run()
  }

}


