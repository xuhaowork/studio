package com.self.core.groupTopN


import com.self.core.baseApp.myAPP

import scala.collection.mutable.ArrayBuffer

object groupTopN extends myAPP{
  override def run(): Unit = {
    val rdd = sc.parallelize(List((2, 3), (1, 3), (2, 1), (2, 5), (1, 1), (2, 4), (2, 3), (1, 2), (1, 4), (1, 5))).map((_, 1))

    val group_rdd = rdd.reduceByKey(_ + _).map(x => (x._1._1, (x._1._2, x._2)))

    group_rdd.foreach(println)
    // method 1: top n by Iterator
    val topN_rdd1 = group_rdd.groupByKey().map(x => {
      val key = x._1
      // as Iterator
      val valueIterator = x._2.iterator
      var buffer = ArrayBuffer((0, -1), (0, -1), (0, -1))
      // loop
      while(valueIterator.hasNext){
        val value = valueIterator.next
        buffer = if(value._2 > buffer(0)._2){
          buffer(0) = value
          buffer
        }else if(value._2 > buffer(1)._2){
          buffer(2) = buffer(1)
          buffer(1) = value
          buffer
        }else if(value._2 > buffer(2)._2){
          buffer(2) = value
          buffer
        }else{
          buffer
        }
      }
      (key, buffer.toList)
    })
    topN_rdd1.foreach(println)


    // method2: top n by collect
    val topN_rdd2 = group_rdd.groupByKey().map(x => {
      val key = x._1
      // collect in each executor
      val valueList = x._2.toList

      val sortList = valueList.sortWith(_._2 > _._2).take(3)
      (key, sortList)
    })
    topN_rdd2.foreach(println)











  }

}
