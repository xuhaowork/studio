package com.self.core.testBaseApp

import com.self.core.baseApp.myAPP
import org.apache.spark.sql.types.StructType
object testBaseApp extends myAPP{
  override def run(): Unit = {
    // Test compile.
    println("Hello world!")

//    val divide: PartialFunction[Int, Int] = {case x: Int => 100/x}
//    println(divide(0))

    val nsqrt = new PartialFunction[Int, Double] {
      def isDefinedAt(x: Int): Boolean = x >= 0
      def apply(v1: Int): Double = scala.math.sqrt(v1)
    }


    println(nsqrt(-1))

    println(scala.math.sqrt(-1))




    import java.io.PrintWriter
    import scala.reflect.io.Directory


    import java.io.File

    val path = new File("./")
    path.listFiles().foreach(file => println(file.getAbsolutePath))


















    //    // Test SparkContext.
//    val rdd = sc.parallelize((1 to 100).toList)
//    val rdd_sum = rdd.reduce(_ + _)
//    println(rdd_sum)

    import breeze.linalg.svd
    import org.apache.spark.mllib.classification.LogisticRegressionWithSGD


  }

}
