package com.self.core.learningImplicit

object newInt {
  implicit class Fishes(val x: Int){
    def fishes: String = {
      "Fish"*3
    }
  }
}
