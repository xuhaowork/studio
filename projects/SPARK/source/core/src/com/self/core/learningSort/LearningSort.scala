package com.self.core.learningSort

import com.self.core.baseApp.myAPP


/**
  * Created by DataShoe on 2018/1/8.
  * learning a document -- Ordering, Ordered and implicit in scala
  */
object LearningSort extends myAPP{
  override def run(): Unit = {
    /**
      * 基本排序方法
      */
//    // 基本类型排序 --基本类型都继承了Ordered类
//    val newArray = Array(1, 2, 4, 6, 7, 8)
//    newArray.sorted(scala.Ordering.Int on {x: Int => -x}).foreach(println)
//
//
//
//    // 引入implicit Ordering
//    implicit val newOrdering = new Ordering[Int]{
//      override def compare(x: Int, y: Int): Int = {
//        -x.compare(y)
//      }
//    }
//
//    println("reformat the implicit Ordering")
//    newArray.sorted.foreach(println)


//    newArray.sortWith(_ > _).foreach(println)
//    newArray.sortBy(x => -x).foreach(println)
//    // 当然也可以加reverse起到同样效果。
//
    // 元组[基本类型排序]
//    val newArray2 = Array((1, 3), (1, 1), (2, 1), (4, 2), (5, 3), (5, 1), (4, 2))
//    newArray2.sorted(Ordering[(Int, Int)] on {x: (Int, Int) => (x._1, -x._2)}).foreach(println)



//    // 集合基本类型排序
//    val newArray3 = Array(List(1, 3), List(1, 1), List(2, 1), List(4, 2), List(5, 3), List(5, 1), List(4, 2))
//    newArray3.sorted.foreach(println) // not compile
//

//    /**
//      * 加入implicit参数
//      */
//    // 基本类型排序 --基本类型都继承了Ordered类
//    val newArray11 = Array(1, 2, 4, 6, 7, 8)
//    newArray11.sorted(scala.Ordering.Int on {x: Int => -x}).foreach(println)
//
//
//    // 引入implicit Ordering
//    implicit val newOrdering = new Ordering[Int]{
//      override def compare(x: Int, y: Int): Int = {
//        -x.compare(y)
//      }
//    }
//    println("reformat the implicit Ordering")
//    newArray11.sorted.foreach(println)
//
//
//    // case class类型排序
//    case class Person(id: Int, age: Int)
//    val newArray4 = Array(Person(1, 3), Person(1, 1), Person(2, 1), Person(4, 2), Person(5, 3), Person(5, 1), Person(4, 2))
////    newArray4.sorted.foreach(println) // not compile

//
//    implicit val newPersonOrdering = new Ordering[Person]{
//      override def compare(x: Person, y: Person): Int = {
//        val ord1 = x.id.compare(y.id)
//        if(ord1 != 0) -ord1 else x.age.compare(y.age)
//      }
//    }
//
//    newArray4.sorted.foreach(println) // compile
//
//

//    import com.self.core.learningSort.List
//    val newList = Array(List(0, 19), List(1, 1), List(10, 7), List(4, 1), List(4, 19), List(4, 1), List(0, 0), List(10, 9))
//    newList.sorted.foreach(println) // not compile

//    implicit val newListOrdering = new Ordering[List[Int]]{
//      override def compare(x: List[Int], y: List[Int]): Int = {
//        var i = 0
//        var flag = 0
//        while(flag.equals(0) && i < x.length){
//          val ord = x(i).compare(y(i))
//          if(ord != 0)
//            flag = ord
//          i += 1
//        }
//        -flag
//      }
//    }
//
//    import listOrder._


//    val newList = Array(List(0, 19), List(1, 1), List(10, 7), List(4, 1), List(4, 19), List(4, 1), List(0, 0), List(10, 9))
////    newList.sorted.foreach(println) // not compile
//
//    import scala.math.Ordering
//    object listOrder{
//      class listOrdering extends Ordering[List[Int]]{
//        override def compare(x: List[Int], y: List[Int]): Int = {
//          var i = 0
//          var flag = 0
//          while(flag.equals(0) && i < x.length){
//            val ord = x(i).compare(y(i))
//            if(ord != 0)
//              flag = ord
//            i += 1
//          }
//          flag
//        }
//      }
//      implicit object List extends listOrdering
//    }
//    import listOrder._
//    newList.sorted.foreach(println) // compiled






//    // 继承Ordered
//    case class Person(age:Int, name: String) extends Ordered[Person]
//    {
//      def compare(that: Person) =
//        if(this.age.compare(that.age) == 0)
//          this.name.compare(that.name)
//        else this.age.compare(that.age)
//    }
//
//    val arr = Array(Person(19, "Json"), Person(21, "Jackson"), Person(13, "Lisa"), Person(61, "Tom"))
//    arr.sorted.foreach(println)
//
//
//    /**
//      * 利用隐含变量Ordering[class]
//      */
//    implicit val newOrdering = new Ordering[Person]{
//      override def compare(x: Person, y: Person): Int = {
//        - x.compare(y)
//      }
//    }
//    println("another implicit declare")
//    arr.sorted.foreach(println)



    // case class类型排序
    class Person(val id: Int, val age: Int)
    val newArray4 = Array(new Person(1, 3), new Person(1, 1), new Person(2, 1), new Person(4, 2), new Person(5, 3), new Person(5, 1), new Person(4, 2))
//        newArray4.sorted.foreach(println) // not compile

    object valueBox {
      class personOrdering extends Ordering[Person] {
        override def compare(x: Person, y: Person): Int = {
          val ord1 = x.id.compare(y.id)
          if (ord1 != 0) -ord1 else x.age.compare(y.age)
        }
      }

      implicit object Person2 extends personOrdering
    }

    import valueBox._
    newArray4.sorted.foreach(println) // not compile










    //    /**
//      * define a case class which needs to be sorted
//      */
//    case class Value(i: Int) extends Ordered[Value] {
//      def compare(that: Value) = this.i - that.i
//    }
//
//    val valueList = List(Value(1), Value(2), Value(3), Value(2), Value(1))
//    // sort
//    println(valueList.min)
//    valueList.sorted.foreach(println)
//
//
//
//
//
//
//
//    /**
//      * define a box for any class
//      */
//    trait Box[T] {
//      def value: T
//    }
//
//    /**
//      * define class with sorting method
//      */
//    class Sort[T](ordering: Ordering[Box[T]]) {
//      def apply(boxes: Seq[Box[T]]) = {
//        boxes.sorted(ordering)
//      }
//    }
//
//    class BoxOrdering[T](ordering: Ordering[T]) extends Ordering[Box[T]] {
//      def compare(x: Box[T], y: Box[T]): Int = ordering.compare(x.value, y.value)
//    }
//
//
//    /**
//      * use Ordering classes to sort a list consist of type A
//      */
//
//    case class IntBox(value: Int) extends Box[Int]
//    val list = List(IntBox(1), IntBox(2), IntBox(3), IntBox(2), IntBox(1))
//    list.sorted(new BoxOrdering(scala.math.Ordering.Int)).foreach(println)
//
////    val sort = new Sort(new BoxOrdering(scala.math.Ordering.Int))
//
////    sort(list).foreach(println)
//
//
//
//    object boxSort {
//      def apply[T](boxes: Seq[Box[T]])(implicit ordering: Ordering[T]) = {
//        val boxOrdering = new BoxOrdering(ordering)
//        boxes.sorted(boxOrdering)
//      }
//    }
//
//
//    // another implicit declare
//    implicit val newOrdering = new Ordering[Int]{
//      override def compare(x: Int, y: Int): Int = {
//        - x.compare(y)
//      }
//    }
//
//    println("another implicit declare")
//    boxSort(list).foreach(println)
















  }

}
