package cn.datashoe.base

abstract class APP {
  def run(): Unit

  def main(args: Array[String]): Unit = {
    run()
  }

}
