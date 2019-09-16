package com.self.core.syntax

/**
  * @author datashoe
  * @version 1.0
  */

class A {
  @deprecated(message = "this method is instead by a(param1: Int, param2: Int)", "1.0")
  def a(param1: Int, param2: Int, param3: Boolean = true): Int = {
    param1 + param2 + 1
  }

  def a(param1: Int, param2: Int): Int = {
    a(param1: Int, param2, true)
  }

}
