package com.self.core.testBaseApp


import com.self.core.baseApp.myAPP
import org.joda.time.DateTime
//import com.self.core.syntax.learningPrivate
import scala.collection.mutable.ArrayBuffer



object testMyApp extends myAPP with Serializable {
  override def run(): Unit = {
//    // Test compile.
//    println("Hello world!")
//
//    // Test SparkContext.
//    val rdd = sc.parallelize((1 to 100).toList)
//      .map(x => (x.toDouble, x.toDouble, x.toString))
//
//    // Change the rdd via the class with private param
////    val param = new learningPrivate()
////      .set_height(150.0)
////      .set_weight(171.5)
////      .set_name("DataShoe")
////
////    param.changRdd(rdd)
//
//    // Change the rdd via the companion object
//    val newRdd = learningPrivate.train("DataShoe", 171.5, 155.5, rdd)
//    newRdd.foreach(println)
//
//    val u = Array(1.0, 2.0, 3.0, 4.0, 5.0)

//    val arr1 = Array.range(0, 5)
//    val arr2 = Array.range(0, 10, 2)
//
//    val u = Array.concat(arr1, arr2)
//    u.foreach(println)
//
//
//    println("-"*80)
//    Array.iterate(0, 5)(x => x + 3).foreach(println)
//
//    println("-"*80)
//    val newArr = Array.empty[Int] ++ {for(i <- 0 until 5) yield 3*i}
//    val newArr2 = ArrayBuffer.empty[Int] ++ {for(i <- 0 until 5) yield 3*i}
//    newArr2.foreach(println)
//
//    println("-"*80)
//    Array.tabulate(10)(i => Array.fill(i + 1)(0.0)).foreach(x => println(x.length))
//
//
//    println("-"*80)
//    val multiTable = Array.tabulate(9, 9)((i, j) => f"${j + 1}%2d * ${i + 1}%2d = ${(i + 1)*(j + 1)}%2d")
//    multiTable.foreach(arr => println(arr.mkString("  ")))
//
//    val triMultiTable = Array.tabulate(9)(i => Array.range(1, i + 2).map(j => f"$j%2d * ${i + 1}%2d = ${(i + 1) * j}%2d"))
//    triMultiTable.foreach(arr => println(arr.mkString("  ")))
//
//    println("-"*80)
//    triMultiTable.foreach(x => println(x.length))


    println(Integer.valueOf("0000000000000000001", 2))


//    import java.text.SimpleDateFormat
//    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val u = format.parse("2018-3-2 00:10:00")
//    val dt = new DateTime(u.getTime)
//
//    println(dt)
//    println(dt.monthOfYear().roundFloorCopy())

//    val m = Array("A", "B", "C", "D", "E", "F", "G",
//      "H", "I", "J", "K", "L", "M", "N")
//    val arr = Array.tabulate(1000)(i => if(i < 3) (i, 100) else (i % 14, i / 50))
//    val rdd = sc.parallelize(arr, 10)
//
//    val groupRdd = rdd.map(arr => (m(arr._1), arr._2))
//////    groupRdd.groupByKey().foreach(println)
////
////    val newRdd = groupRdd.groupByKey().mapValues(iter => {
////      val filterIter = iter.filter(i => i >= 100)
////      filterIter
////    })
////    newRdd.foreach(println)
//
//    /**
//      * 找出大于100的值
//      */
//    groupRdd.mapPartitions(iterator => {
//      val result = ArrayBuffer.empty[(String, Int)]
//      while(iterator.hasNext){
//        if(iterator.next()._2 >= 100)
//          result += iterator.next()
//      }
//      result.toIterator
//    }).foreach(println)


    /**
      * 例子2, mapPartition起到了过滤的作用
      */
//    val arr2 = Array(("A", 100), ("A", 1), ("A", 2), ("A", 3), ("A", 40),
//      ("B", 10), ("B", 1), ("B", 2), ("B", 3), ("B", 40),
//      ("C", 100), ("C", 1), ("C", 2), ("C", 3), ("C", 40))
//    val rdd2 = sc.parallelize(arr2)
//    val result = rdd2.mapPartitions(iterator => {
//      val result = ArrayBuffer.empty[(String, Int)]
//      while(iterator.hasNext){
//        val nt = iterator.next()
//        if(nt._2 >= 101){
//          result += nt
//        }
//      }
//      result.toIterator
//    })
//
//    result.foreach(println)
//    println(result.count())



//    /**
//      * 例子3, mapPartition起到了过滤的作用
//      */
//    val arr2 = Array(("A", 100), ("A", 1), ("A", 2), ("A", 3), ("A", 40),
//      ("B", 10), ("B", 1), ("B", 2), ("B", 3), ("B", 40),
//      ("C", 100), ("C", 1), ("C", 2), ("C", 3), ("C", 40))
//    val rdd2 = sc.parallelize(arr2)
//    val result = rdd2.mapPartitions(iterator => {
//      val result = ArrayBuffer.empty[(String, Int)]
//      while(iterator.hasNext){
//        val nt = iterator.next()
//        if(nt._2 >= 100){
//          result += nt
//          result += iterator.next()
//          result += iterator.next()
//          result += iterator.next()
//        }
//      }
//      result.toIterator
//    })
//
//    result.foreach(println)
//    println(result.filter(_._1 == "B").count())

    /**
      * 例子4
      */
    val m = Array("A", "B", "C", "D", "E", "F", "G",
      "H", "I", "J", "K", "L", "M", "N")
    val arr = Array.tabulate(1000)(i => if(i < 3) (i, 100) else (i % 14, i / 50))
    val rdd = sc.parallelize(arr, 10)

    val groupRdd = rdd.map(arr => (m(arr._1), arr._2))
    //    groupRdd.groupByKey().foreach(println)

    val newRdd = groupRdd.groupByKey().mapValues(iter => {
      val filterIter = iter.filter(i => i >= 100)
      filterIter
    })
    newRdd.foreach(println)

//    /**
//      * 找出大于100的值
//      */
//    groupRdd.mapPartitions(iterator => {
//      iterator.filter(_._2 >= 100)
//    }).foreach(println)


    groupRdd.mapPartitions(iterator => {
      val result = ArrayBuffer.empty[(String, Int)]
      while(iterator.hasNext){
        if(iterator.next()._2 >= 100)
          result += iterator.next()
      }
      result.toIterator
    }).foreach(println)

  }

}
