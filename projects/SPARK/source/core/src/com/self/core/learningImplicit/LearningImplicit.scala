package com.self.core.learningImplicit

import com.self.core.baseApp.myAPP


/**
  * Created by DataShoe on 2018/1/8.
  */
object LearningImplicit extends myAPP{
  override def run(): Unit = {
    def add(x: Int)(implicit y: Int) = x + y


    println(add(3)(4))

    implicit val z: Int = 4
    println(add(5))


    import newInt._
    println(3.fishes)

    println("*"*3)


    val rdd = sc.parallelize(List(0, 1, 2, 3))
    rdd.takeOrdered(3).foreach(println)

    import org.apache.spark.sql.DataFrame

  }


}
