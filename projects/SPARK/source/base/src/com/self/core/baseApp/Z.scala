package com.self.core.baseApp

/**
  * 为了保持和deepinsight编译环境一直
  */
class Z {
  private val rddList = new java.util.LinkedHashMap[String, AnyRef]

  def rdd(rddName: String): Any = rddList.get(rddName)
}
