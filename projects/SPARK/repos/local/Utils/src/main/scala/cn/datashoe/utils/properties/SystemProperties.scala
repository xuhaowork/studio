package cn.datashoe.utils.properties

import java.io.File

/**
  * 一些系统变量
  */
object SystemProperties {
  /** 运行时的当前目录 */
  lazy val path: String = new File("").getAbsolutePath.replaceAll("\\\\", "\\/")

  /** SPARK repo的根目录 --需要在系统的环境变量中设定[MY_WORKPLACE_SPARK]变量并使其生效 */
  lazy val MY_WORKPLACE_SPARK: String = scala.util.Try(System.getenv("MY_WORKPLACE_SPARK")).getOrElse(path)

}
