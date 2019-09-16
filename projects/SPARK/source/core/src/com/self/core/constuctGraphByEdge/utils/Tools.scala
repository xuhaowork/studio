package com.self.core.constuctGraphByEdge.utils

import org.apache.avro.SchemaBuilder.ArrayBuilder

import scala.collection.mutable
import scala.util.matching.Regex

object Tools extends Serializable {
  /**
    * ip和long类型互转算法
    * ----
    * 原理: ip是由[.]分隔的四组[0-255]的数构成，也就是每个数均为小于2的8次方的自然数.
    * 编码: 每迭代一个数左移8位，倒叙迭代四次
    * 解码: 255最后8位全为1，将数值与255进行按位与运算，并将数值右移8位，正序迭代4次
    */
  def ipEncode2Long(ip: String): Long = {
    var res = 0L
    var i = 0
    ip.split("\\.", 4).foreach {
      s =>
        val num = try {
          s.trim.toLong
        } catch {
          case _: Exception => throw new Exception(s"在解析ip时出现异常，您的ip：'$ip'不是标准ip")
        }
        require(num >= 0L && num < 256L, s"在解析ip时出现异常，您输入的ip：'${ip}'不是标准ip")
        res |= num << (i * 8)
        i += 1
    }
    res
  }

  def numEncode2Ip(num: Long): String = {
    var res = (num & 255).toString
    var i = 1
    while (i < 4) {
      res += "." + ((num >> (i * 8)) & 255L).toString
      i += 1
    }
    res
  }

//  def hashEncodeStringByLength(string: String)(length: Int = 20): Long = {
//
//
//  }


  class StringEncoder(val charSet: String = "UTF-8", maxLength: Int) extends Serializable {

    if(charSet == "ascii" || charSet == "ISO-8859-1"){
      require(maxLength < 16)
    } else {
      require(maxLength < 8)
    }

    var requireFitLength = true

    val supportCharSets = Array("ascii", "ISO-8859-1", "gb2312", "gbk", "unicode", "UTF-8")

    def setRequireFitLength(throwsException: Boolean): this.type = {
      this.requireFitLength = throwsException
      this
    }


    def encode(string: String): Long = {
      if(requireFitLength){
        require(string.length <= maxLength, s"您输入字符串'$string'长度'${string.length}'超过了最大长度限制：'$maxLength'")
      }
      var res = 0L
      var i = 0
      string.getBytes(charSet).foreach {
        bt =>
          val num = if(charSet == "ascii" || charSet == "ISO-8859-1") bt.toInt else bt.toInt + 128 // 由-128至127映射到0到255
          res |= num << (i * 8) // 倒叙循环顺次左移8位
          i += 1
      }
      res
    }

    def decode(code: Long): String = {
      var array = mutable.ArrayBuilder.make[Byte]()
      var num = code

      while(num >= 0) {
        array += (num & 255L).toByte
        num = num >> 8
      }
      new String(array.result(), charSet)
    }


  }



  /**
    * 判定ip是否是内网
    * ----
    * 内网判断规则：
    * 1)10网段
    * 2)192.168网段
    * 3)172.[16-31]网段, 这里有点歧义还需要沟通：应该是包括[20-29]网段的了，但是否包括[200-255]等网段？
    *
    * @param ip ip地址
    * @return 是否是内网
    */
  def isInnerIp(ip: String): Boolean = {
    val pattern1 = "(10)\\.([0-2]?[0-9]{1,2})\\.([0-2]?[0-9]{1,2})\\.([0-2]?[0-9]{1,2})"
    val pattern2 = "192\\.168\\.([0-2]?[0-9]{1,2})\\.([0-2]?[0-9]{1,2})"
    val pattern3 = "172\\.(1[6-9]||2[0-9]||3[0-1])\\.([0-2]?[0-9]{1,2})\\.([0-2]?[0-9]{1,2})"
    val patterns = Array(pattern1, pattern2, pattern3)

    var result: Boolean = false
    var index = 0
    while (!result && index < patterns.length) {
      result = new Regex(patterns(index)).pattern.matcher(ip).matches()
      index = index + 1
    }
    result
  }


}
