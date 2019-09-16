package cn.datashoe.dataWrangling


object StringWrangling extends Serializable {
  /**
    * float格式化为String
    *
    * @param precision 有效位数
    * @param scale     小数部分位数
    * @param number    数值
    * @return 格式化的字符串
    */
  def floatFormat(precision: Int, scale: Int)(number: Double): String = {
    val format = if (precision <= 0)
      s"%.${scale}f%%"
    else
      s"%$precision.${scale}f%%"
    format.format(number)
  }

}
