package com.self.core.geoHash

import scala.collection.mutable.ArrayBuffer

/**
  * --------
  * GeoHash算法
  * --------
  * 实现：单点编码、九点编码、单点解码、九点解码
  */
class GeoHash(val hashLength: Int = 9) extends Serializable {
  /** Base32编码 */
  val digits: Array[Char] = Array(
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm',
    'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
    'y', 'z')

  /**
    * 经纬度范围
    */
  val longitudeRange: (Double, Double) = (-180.0, 180.0)
  val latitudeRange: (Double, Double) = (-90.0, 90.0)

  /** 单点编码
    * ----
    * @param longitude 经度
    * @param latitude 纬度
    * @return geoHash字符串
    */
  def encode(longitude: Double, latitude: Double): String = {
    var i = 0
    var flag = true // true时对纬度二分, 否则对经度二分, 以经度二分开头
    var (lgLower: Double, lgUpper: Double) = longitudeRange
    var (laLower: Double, laUpper: Double) = latitudeRange
    var bitNum = 0
    var hashString = ""
    while(i < hashLength * 5){
      if(flag){
        val lgMid: Double = (lgLower + lgUpper) / 2.0
        if(longitude >= lgMid){
          lgLower = lgMid
          bitNum = bitNum * 2 + 1
        }else{
          lgUpper = lgMid
          bitNum = bitNum * 2
        }
      }else{
        val laMid: Double = (laLower + laUpper) / 2
        if(latitude >= laMid){
          laLower = laMid
          bitNum = bitNum * 2 + 1
        }else{
          laUpper = laMid
          bitNum = bitNum * 2
        }
      }
      if(i % 5 == 4){
        hashString += digits(bitNum)
        bitNum = 0
      }
      flag = !flag
      i += 1
    }
    hashString
  }

  /** 已知长度： 经纬度 => 九点编码
    * ----
    * @param longitude 经度
    * @param latitude 纬度
    * @return geoHash字符串
    */
  def encodeFor9(longitude: Double, latitude: Double): String = {
    /** 编码为Array[Int]的形式 */
    val arr = encodeArr(longitude: Double, latitude: Double)
    /** 求周围的九个点的值 */
    Array(
      encodeForSurround(arr, (-1, 1)), // 左侧上中下三个点
      encodeForSurround(arr, (-1, 0)),
      encodeForSurround(arr, (-1, -1)),

      encodeForSurround(arr, (0, 1)), // 中间上中下三个点
      arr.map(i => digits(i)).mkString(""),
      encodeForSurround(arr, (0, -1)),

      encodeForSurround(arr, (1, 1)), // 右侧上中下三个点
      encodeForSurround(arr, (1, 0)),
      encodeForSurround(arr, (1, -1))).mkString(",")
  }


  /**
    * 单点编码
    * ----
    * @param longitude 经度
    * @param latitude 纬度
    * @return 经纬度二进制迭代后拆分为五位二进制数的数组形式，这里直接用Int标识
    */
  private def encodeArr(longitude: Double, latitude: Double): ArrayBuffer[Int] = {
    var i = 0
    var flag = true // true时对纬度二分, 否则对经度二分, 以经度二分开头
    var (lgLower: Double, lgUpper: Double) = longitudeRange
    var (laLower: Double, laUpper: Double) = latitudeRange
    var bitNum = 0
    var hashArr = ArrayBuffer.empty[Int]
    while(i < hashLength * 5){
      if(flag){
        val lgMid: Double = (lgLower + lgUpper) / 2.0
        if(longitude >= lgMid){
          lgLower = lgMid
          bitNum = bitNum * 2 + 1
        }else{
          lgUpper = lgMid
          bitNum = bitNum * 2
        }
      }else{
        val laMid: Double = (laLower + laUpper) / 2
        if(latitude >= laMid){
          laLower = laMid
          bitNum = bitNum * 2 + 1
        }else{
          laUpper = laMid
          bitNum = bitNum * 2
        }
      }
      if(i % 5 == 4){
        hashArr += bitNum
        bitNum = 0
      }
      flag = !flag
      i += 1
    }
    hashArr
  }

  /**
    * 为周围的九个点进行编码
    * ----
    * @param arr 中心店编码的值
    * @param plusGrid 上下移动的格子, 上移就是1, 下移就是-1, 否则就是0.
    *                 for example: (1, -1)就是经度方向右移一格，纬度方向左移一格
    * @return 九点编码的hash字符串
    */
  private def encodeForSurround(arr: ArrayBuffer[Int], plusGrid: (Int, Int)): String = {
    var flag = if(hashLength % 2 == 1) true else false // hashLength为奇数时末尾为经度
    var plus = plusGrid // 经纬度的上下格数
    var buff = ""
    for(each <- arr.reverse){
      val binaryString = Integer.toBinaryString(each + 32).substring(1, 6)
      var i = 0
      var num = 0
      while(i < 5){
        val binaryNum = binaryString.charAt(4 - i) - 48
        if(flag){
          if(binaryNum + plus._1 >= 2){
            plus = (1, plus._2)
          }else if(binaryNum + plus._1 < 0){
            num += 1 << i
            plus = (-1, plus._2)
          }else{
            num += (binaryNum + plus._1) * (1 << i)
            plus = (0, plus._2)
          }
        }else{
          if(binaryNum + plus._2 >= 2){
            plus = (plus._1, 1)
          }else if(binaryNum + plus._2 < 0){
            num += 1 << i
            plus = (plus._1, -1)
          }else{
            num += (binaryNum + plus._2) * (1 << i)
            plus = (plus._1, 0)
          }
        }
        flag = !flag
        i += 1
      }
      buff += digits(num)
    }
    buff.reverse
  }


}


