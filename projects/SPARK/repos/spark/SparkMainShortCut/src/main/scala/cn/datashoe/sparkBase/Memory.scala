package cn.datashoe.sparkBase

import java.io.File
import java.util

/**
  * 存储变量的工具类
  * ----
  * 1)当前目录
  * 2)工作目录
  * 3)脚本间输入输出的模拟存储
  * 4)模拟的输入输出接口
  */
object Memory {
  /** 运行时的当前目录 */
  lazy val path: String = new File("").getAbsolutePath.replaceAll("\\\\", "\\/")

  /** SPARK repo的根目录 --需要在系统的环境变量中设定[MY_WORKPLACE_SPARK]变量并使其生效 */
  lazy val MY_WORKPLACE_SPARK: String = scala.util.Try(System.getenv("MY_WORKPLACE_SPARK")).getOrElse(path)

  private val REPOSITORY: util.HashMap[String, Any] = new java.util.HashMap[java.lang.String, Any]()

  /** 输入 */
  def input(key: String, value: Any): Unit = {
    REPOSITORY.put(key, value)
  }

  /** 输出 */
  def output[T](key: String, value: Any): T = try {
    REPOSITORY.get(value).asInstanceOf[T]
  } catch {
    case e: Throwable => throw new IllegalAccessError(s"在获得${key}的过程中失败: ${e.getMessage}")
  }

}
