package com.self.core.hashAlgorithm

import java.math.BigInteger

import com.self.core.baseApp.myAPP

object HashAlgorithm extends myAPP{
  override def run(): Unit = {
//    val str = "1313130"
////    println(BigInteger.valueOf((97 << 7).toLong))
////    println('a'.toByte)
//
//    val mask = new BigInteger("2").pow(32).subtract(BigInteger.valueOf(1l))
//    val m = new BigInteger("1").shiftLeft(64).subtract(new BigInteger("1"))
//    println(mask)
//    println(m)

    val arr1 = Array("today", "is", "a good day")
    val arr2 = arr1
    val finger1 = new SimHash(64).simHash(arr1)
    val finger2 = new SimHash(64).simHash(arr2)

    println(finger1)
    println(finger2)

    println(new SimHash(64).hammingDistance(finger1, finger2, 64))


//    mask
//    println(Integer.valueOf(mask.toString()))
    /*code:
    public int hashCode() {
      int h = hash;
      if (h == 0 && value.length > 0) {
        char val[] = value;

        for (int i = 0; i < value.length; i++) {
          h = 31 * h + val[i];
        }
        hash = h;
      }
      return h;
    }
    */






  }
}
