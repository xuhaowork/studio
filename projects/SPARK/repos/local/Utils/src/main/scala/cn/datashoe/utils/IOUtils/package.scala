package cn.datashoe.utils

import java.util.Locale

import scala.collection.mutable

package object IOUtils {
  trait Writer {
    protected var shouldOverwrite: Boolean = false

    def save(path: String): Unit = {
      saveImpl(path)
    }

    protected def saveImpl(path: String): Unit

    /**
      * Map to store extra options for this writer.
      */
    protected val optionMap: mutable.Map[String, String] = new mutable.HashMap[String, String]()

    def option(key: String, value: String): this.type = {
      require(key != null && !key.isEmpty)
      optionMap.put(key.toLowerCase(Locale.ROOT), value)
      this
    }

    def overwrite(): this.type = {
      shouldOverwrite = true
      this
    }

  }


  /**
    * 示例: 按行读取
    */
  object ReadByLine {
    def main(args: Array[String]): Unit = {
      import scala.io.Source
      val ss = Source.fromFile("F:\\My_Workplace\\data\\LANL-Earthquake-Prediction\\train.csv").getLines()
      for (each <- ss) {
        println(each)
      }

    }

  }


}
