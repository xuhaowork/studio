package src.com.self.core.geoHash

/**
  * editor：Xuhao
  * date： 2017/7/14 10:00:00
  */

/**
  * --------
  * 备注：
  * --------
  * 实现GeoHash算法
  * 具体算法原理参考："https://www.cnblogs.com/tgzhu/p/6204173.html"
  * 实现：编码到二进制数、编码到字符串、从二进制数解码、从字符串解码方法
  */
class GeoHashMap(val hashLength: Int = 9) extends Serializable{
  // The bits number of the latitude or longitude coding.
  val numbase32: Int = hashLength/2

  // Base32 coding
  var digits = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')

  // Convert the latitude and longitude to a binary coding.
  def getBits(lat: Double, floorr: Double, ceilingr: Double, iteration: Int) = {
    var floor: Double = floorr
    var ceiling: Double = ceilingr
    var buffer = new Array[Int](iteration)
    for (i <- 1 to iteration) {
      var mid = (floor + ceiling) / 2
      if (lat >= mid) {
        buffer(i-1)= i
        floor = mid
      } else {
        ceiling = mid
      }
    }
    buffer
  }

  // Transform a binary number(a string type of an int) to a decimal number.
  def binaryToDecRecur(src: String): Long = {
    if(!src.matches("[0|1]*")){
      return 0
    }
    src.length match {
      case 0 => 0
      case 1 => src.toLong * Math.pow(2,0).toLong
      case _ => binaryToDecRecur(src.substring(1,src.length)) +
        src.substring(0,1).toLong * (1 << src.length - 1)
    }
  }

  // Transform a decimal number(int) to a binary string.
  def DecToBinary(i: Int) = {
    var decNum: Int = i
    var src = new StringBuilder
    while(decNum/2 != 0){
      src.append(decNum%2)
      decNum /= 2
    }
    src.append(decNum%2)
    src.reverse
  }

  // Encoding the latitude and longitude to a string.
  def encode(lat: Double, lon: Double): String = {
    var buffer = new StringBuilder()
    var arrTest = new StringBuilder()
    if(hashLength%2 != 0){
      val lonbits = getBits(lon, -180, 180, numbase32*5 + 3)
      val latbits = getBits(lat, -90, 90, numbase32*5 + 2)
      for(i <- 0 until numbase32*5 + 2){
        buffer.append(lonbits(i) match {
          case 0 => "0"
          case _ => "1"
        })
        buffer.append(latbits(i) match {
          case 0 => "0"
          case _ => "1"
        })
      }
      buffer.append(lonbits(numbase32*5 + 2) match {
        case 0 => "0"
        case _ => "1"
      })
    }else{
      val latbits = getBits(lat, -90, 90, numbase32*5)
      val lonbits = getBits(lon, -180, 180, numbase32*5)
      for(i <- 0 until numbase32*5){
        buffer.append(lonbits(i) match {
          case 0 => "0"
          case _ => "1"
        })
        buffer.append(latbits(i) match {
          case 0 => "0"
          case _ => "1"
        })
      }
    }
    for(i <- 0 until hashLength){
      var n = binaryToDecRecur(buffer.substring(i*5, i*5 + 5)).toInt
      arrTest.append(digits(n))
    }
    arrTest.toString() // We can also get the a type of array.
  }

  // Create a map of digits and is index for decoding of the geoHash string.
  var digitsMap: Map[Char, Int] = Map()
  for(i <- 0 until digits.length){
    var charelem = digits(i)
    digitsMap += (charelem -> i)
  }

  // Decoding the geoHash array(consist of chars of base32.)
  def decode(geohash: String) = {
    var buffer = new StringBuilder()
    var arrLon = scala.collection.mutable.Buffer[Int]()
    var arrLat = scala.collection.mutable.Buffer[Int]()
    for(each <- geohash){
      var i: Int = digitsMap(each) + 32
      buffer.append(DecToBinary(i).substring(1))
    }
    var i = 0
    while(i < buffer.length){
      if(i%2 == 0){
        if(buffer.charAt(i) == '1'){
          arrLon += 1
        }else{
          arrLon += 0
        }
      }else{
        if(buffer.charAt(i) == '1'){
          arrLat += 1
        }else{
          arrLat += 0
        }
      }
      i += 1
    }
    val lat:Double = decodeArr(arrLat.toArray, -90.0, 90.0)
    val lon:Double = decodeArr(arrLon.toArray, -180.0, 180.0)
    (lat, lon)
  }

  // Transform the binary coding array to the longitude and latitude. The function is designed polymorphic.
  def decodeArr(bs: Array[Int], floorr: Double, ceilingr: Double): Double = {
    var mid:Double = 0
    var floor: Double = floorr
    var ceiling: Double = ceilingr
    for(i <- 0 until bs.length){
      mid = (floor + ceiling)/2
      if(bs(i) == 1){
        floor = mid
      }else{
        ceiling = mid
      }
    }
    mid
  }

  // Compute the distance (kilometers) of two points which are marked by the axis of latitude and longitude.
  def getDistance(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
    if(lat1!=0 && lon1!=0 && lat2!=0 && lon2!=0){
      val R =  6378.137
      val radLat1 = lat1* Math.PI / 180
      val radLat2 = lat2* Math.PI / 180
      val a = radLat1 - radLat2
      val b = lon1* Math.PI / 180 - lon2* Math.PI / 180
      val s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2) + Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)))
      BigDecimal(s * R).setScale(10, BigDecimal.RoundingMode.HALF_UP)
    }else{
      BigDecimal(0).setScale(10, BigDecimal.RoundingMode.HALF_UP)
    }
  }

  // Compute the distance of clustered areas which are marked by a GeoHash string.
  def getDistance(geohash1: String, geohash2: String): BigDecimal={
    val tup1 = decode(geohash1)
    val tup2 = decode(geohash2)
    val distance: BigDecimal = getDistance(tup1._1, tup1._2, tup2._1, tup2._2)
    distance
  }
}
