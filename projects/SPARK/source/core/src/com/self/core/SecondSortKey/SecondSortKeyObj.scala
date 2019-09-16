package com.self.core.SecondSortKey

import com.self.core.baseApp.myAPP


object SecondSortKeyObj extends myAPP{
  override def run(): Unit = {
    val rdd = sc.parallelize(List((2.1, 3), (1.3, 3), (2.2, 1), (2.1, 5), (1.3, 1), (2.1, 4), (2.2, 3), (1.3, 2), (1.1, 4), (1.10, 5))).map((_, "value"))

    case class sortOrder(height: Double, age: Int)

    val new_rdd = rdd.map(x => (sortOrder(x._1._1, x._1._2), x._2))

    // implicit value for Ordering
    implicit val st = new Ordering[sortOrder]{
      override def compare(a:sortOrder, b:sortOrder): Int = {
        if(a.height==b.height) a.age.compare(b.age)
        else - a.height.compare(b.height)
      }
    }


    /**
      * normal sort
      */
    rdd.sortByKey().foreach(println)

    /**
      * user defined sort -- by implicit Ordering
       */

    new_rdd.sortByKey().map(x => ((x._1.height, x._1.age), x._2)).foreach(println)


    // user defined sort -- by defined a new class which extends Ordered






    //    // order by key
//    new_rdd.sortByKey(ascending = false)
//      .map{x => {
//      val key = (x._1.first, x._1.second)
//      val value = x._2
//      (key, value)
//    }}.foreach(println)
//
//    // order by normal value
//    new_rdd.sortBy(x => x._1)
//      .map{x => {
//      val key = (x._1.first, x._1.second)
//      val value = x._2
//      (key, value)
//    }}.foreach(println)



  }

}
