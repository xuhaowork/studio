package com.self.core.hashAlgorithm

import java.math.BigInteger

class SimHash(hashBits: Int) extends Serializable {

  def simHash(terms: Array[String]): BigInteger = {
    val buffer = new Array[Int](hashBits).toBuffer
    for(each <- terms) {
      val hashcode = BigInteger.valueOf(each.hashCode)
      val weight = 1
      for (i <- 0 until hashBits) {
        val hash_i = hashcode.and(new BigInteger("1").shiftLeft(i)).signum()
        if (hash_i != 0) {
          buffer(i) += weight
        } else {
          buffer(i) -= weight
        }
      }
    }
    var fingerPrint = new BigInteger("0")
    for (i <- 0 until hashBits) {
      if (buffer(i) >= 0) {
        fingerPrint = fingerPrint.add(new BigInteger("1").shiftLeft(i))
      }
    }
    fingerPrint
  }


  def hammingDistance(finger1: BigInteger, finger2: BigInteger, length: Int = hashBits): Int = {
    var xorFinger = finger1.xor(finger2)
    var count = 0
    while (xorFinger.signum() != 0) {
      xorFinger = xorFinger.and(xorFinger.subtract(new BigInteger("1")))
      count += 1
    }
    count
  }


}
