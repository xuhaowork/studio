package cn.datashoe

import java.io.File
import java.util

object Memory {
  /** 运行时的当前目录 */
  lazy val path: String = new File("").getAbsolutePath.replaceAll("\\\\", "\\/")

  /** SPARK repo的根目录 --需要在系统的环境变量中设定 */
  lazy val rootPath: String = scala.util.Try(System.getenv("MY_WORKPLACE_SPARK")).getOrElse(path)

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
