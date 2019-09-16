package com.self.core.BMJL.distCompute

/**
  * Created by DataShoe on 2018/1/12.
  */
class LCS private(var format: String) extends Serializable{
  def this() = this("LCSequence")

  def set_format(format: String): this.type = {
    this.format = format
    this
  }

  def LCSequence(s1: String, s2: String): Int = {
    dpLCSequence(s1.length, s2.length, s1, s2)
  }

  def LCSubstring(s1: String, s2: String): Int = {
    val u = s2.map(x => s1.map(e => if(x == e) "1" else "0"))
    // 从对角线同时朝两个方向求相似度，当全部迭代完或者最大子串长度大于下次迭代的上限中止：
    // ————因为迭代次数递减。
    var i = 0
    var maxLength = 0
    var flag = true

    while (i < math.max(s1.length, s2.length) && flag) {
      // 求sim1,此时j为行, i+j为列
      val sim1 = if (i < s1.length) {
        var s = ""
        for (j <- 0.until(math.min(s2.length, s1.length - i))) {
          s += u(j)(i + j)
        }
        s.split("0", -1).map(_.length).max
      } else {
        0
      }

      // 求sim2,此时j为列，i + j为行
      val sim2 = if (i < s2.length) {
        var ss = ""
        for (j <- 0.until(math.min(s1.length, s2.length - i))) {
          ss += u(i + j)(j)
        }
        ss.split("0", -1).map(_.length).max
      } else {
        0
      }

      maxLength = Array(sim1, sim2, maxLength).max
      val possibleMaxNextIterator =
        math.max(math.min(s1.length, s2.length - i - 1),
          math.min(s2.length,
            s1.length - i - 1))
      if (maxLength >= possibleMaxNextIterator) {
        flag = false
      }

      i += 1
    }
    maxLength
  }

  def lcs(s1: String, s2: String): Int = {
    format match {
      case "LCSequence" => LCSequence(s1: String, s2: String)
      case "LCSubstring" => LCSubstring(s1: String, s2: String)
      case _ => throw new Exception("format must be either \"LCSequence\"" +
        "or \"LCSubstring\".")
    }
  }

  private def dpLCSequence(i: Int, j: Int, s1: String, s2: String): Int = {
    require(i >= 0 && j>= 0)
    if(i*j == 0){
      0
    }else if(s1.charAt(i - 1) == s2.charAt(j - 1)){
      dpLCSequence(i - 1, j - 1, s1, s2) + 1
    }else{
      math.max(dpLCSequence(i - 1 , j, s1, s2), dpLCSequence(i, j - 1, s1, s2))
    }
  }


}

object LCS{
  def lcs(s1: String, s2: String, format: String): Int = {
    new LCS().set_format(format).lcs(s1, s2)
  }

  def lcs(s1: String, s2: String): Int = {
    new LCS().set_format("LCSequence").lcs(s1, s2)
  }
}

