package com.self.core.BMJL.distCompute

import com.self.core.baseApp.myAPP

import scala.collection.GenIterable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by DataShoe on 2018/1/12.
  */
object testLCS extends myAPP{
  override def run(): Unit = {
    val s1 = "因为所以。"
    val s2 = "为所以。个好"

    // 以s1位列，s2为行形成匹配矩阵，进行比对

    println("maxSubString:")
    println(LCS.lcs(s1: String, s2: String, "LCSubstring"))





    val fillMap = ArrayBuffer.fill(10)("")
    fillMap(0) = "abc"


  }
}
