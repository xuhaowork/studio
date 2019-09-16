package com.self.core.stackLearning

import com.self.core.baseApp.myAPP

import scala.collection.mutable

object StackLearning extends myAPP {
  override def run(): Unit = {
    val q = mutable.Queue[Int]()

    for (i <- 0 until 10) {
      q.enqueue(i)
    }

    println(q)

    for (each <- 0 until 20) {
      val value = if (q.isEmpty)
        None
      else
        Some(q.dequeue())
      println(value)
    }

    println(q)


  }
}
