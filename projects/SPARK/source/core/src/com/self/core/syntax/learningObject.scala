package com.self.core.syntax

//import java.text.SimpleDateFormat
//
import java.text.SimpleDateFormat

import com.self.core.baseApp.myAPP
//import org.apache.spark.deploy.master.Master
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.DataFrame
//import org.joda.time.DateTime


object learningObject extends myAPP with Serializable {
  override def run: Unit = {
    /**
      * class和object的基本方法
      */

    /*    // Traversable, Iterable
        class Singular[A](element: A) extends Traversable[A]{
          // 如果继承trait——Traversable，必须顶一个foreach方法
          def foreach[B](fun: A => B) = fun(element)
        }
        val p = new Singular("Plane")
        p.foreach(println)
        p.foreach(x => println(x.length))
        println(p.head)

        // overloaded
        class OverLoader{
          def print(s: String): Unit = println(s"String: $s")
          def print(s: Int): Unit = println(s"Int: $s")
          def print(s: Seq[String]): Unit = println(s"String: ${s.mkString(", ")}")
        }
        val loader = new OverLoader
        loader.print(1)
        loader.print("1")
        loader.print("1" :: "2" :: "3" :: Nil)

        // import as a new name
        import scala.collection.mutable.{Map => muMap}
        val mp = Map((1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five"))
        val muMp = muMap((1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five"))
        muMp += (6 -> "six")
        muMp.foreach(println)*/


    /**
      * access modifier
      */
    /*   // protect for value
        class User{
          protected val passwd = "123456"
        }

        class newUser extends User{
          val newPasswd = passwd
        }

        val user = new User
        val newUser = new newUser
        println(newUser.newPasswd)

        class Filler(protected val passwd: String){}
        val filler = new Filler("123456")
    //    println(filler.passwd) // not compile

        class newFiller{
          protected val passwd = "123456"
          protected def proprintlnPasswd(): Unit = println(passwd)
          def printlnPasswd(): Unit = println(passwd)
        }

        val newfiller = new newFiller
        println(newfiller.printlnPasswd())
        //    println(newfiller.proprintlnPasswd()) // not compile*/


    // private

    //    object testSerializable extends Serializable{
    //      def main(args: Array[String]): Unit = {
    //        val param = new learningPrivate()
    //          .set_height(150.0)
    //          .set_weight(171.5)
    //          .set_name("DataShoe")
    //
    //        val rdd = sc.parallelize((1 until 100).toList)
    //          .map(x => (x.toDouble, x.toDouble, x.toString))
    //
    ////        rdd.map(_ => (171.5, 75.5, "DataShoe")).foreach(println)
    //        param.changRdd(rdd)
    //
    //        val newRdd = learningPrivate.train("DataShoe", 171.5, 155.5, rdd)
    //        newRdd.foreach(println)


    //    object Transposer{
    //      implicit class TransArr[T](val matrix: Array[Array[T]]){
    //        def transposeee(): Seq[Seq[T]] =
    //        {
    //          Array.range(0, matrix.head.length).map(i => matrix.view.map(_(i)))
    //        }
    //      }
    //
    //      implicit class TransSeq[T](val matrix: Seq[Seq[T]]){
    //        def transposeee(): Seq[Seq[T]] =
    //        {
    //          Array.range(0, matrix.head.length).map(i => matrix.view.map(_(i)))
    //        }
    //      }
    //    }
    //
    //    val matrix = Seq(Seq(0, 1, 0), Seq(0, 0, 1), Seq(1, 0, 0))
    //    matrix.foreach(arr => println(arr.mkString(", ")))
    //
    //    // 转置
    //    import Transposer._
    //    matrix.transposeee().foreach(arr => println(arr.mkString(", ")))
    //
    //
    //
    //    Array.range(0, 10).foreach(println)
    //    val s = Array.range(0, 10)
    //    s.slice(0, s.length).foreach(println)


    //    val arr3: Array[Array[Array[String]]] = Array(
    //      Array(
    //        Array("A", "B", "C", "D"),
    //        Array("E", "F", "G", "H"),
    //        Array("I", "J", "K", "L")
    //      ),
    //      Array(
    //        Array("M", "N", "O", "P"),
    //        Array("Q", "I", "S", "T"),
    //        Array("U", "V", "W", "X"),
    //        Array("Y", "Z", "1", "2")
    //      ),
    //      Array(
    //        Array("3", "4", "5", "6"),
    //        Array("7", "8", "9", "0")
    //      )
    //    )
    //
    //    val flattenArr: Array[Array[String]] = arr3.flatten
    //    println(flattenArr.length)
    //    flattenArr.foreach(x => println(x.mkString(",")))
    //
    //
    //    Array.tabulate(2, 3)((j, k) => j * k).foreach(v => println(v.mkString(",")))
    //
    //    val u = Array.tabulate(2, 3)((j, k) => j * k).flatMap(arr => arr.map(_ => Array.range(0, 7)))
    //
    //    u.foreach(v => println(v.mkString(",")))
    //
    //    val u2 = Array.tabulate(2, 3)((j, k) => j * k).map(arr => arr.map(_ => Array.range(0, 7))).flatten
    //
    //    u2.foreach(v => println(v.mkString(",")))


    //    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //    val timeStamp = timeFormat.parse("2017-12-10 00:00:00").getTime
    //    val dateTime = new DateTime(timeStamp)
    ////
    ////    println(dateTime)
    ////    val floorTime = dateTime.monthOfYear().roundFloorCopy()
    ////    println(floorTime.toString("yyyy-MM-dd HH:mm:ss"))
    ////    println(new DateTime(dateTime.getMillis - floorTime.getMillis).hourOfDay().roundHalfFloorCopy().toString("yyyy-MM-dd HH:mm:ss"))
    ////
    ////    println(new DateTime(dateTime.getMillis - floorTime.getMillis).dayOfYear().roundHalfFloorCopy().toString("yyyy-MM-dd HH:mm:ss"))
    //
    //
    //
    //
    //    println(dateTime.hourOfDay().roundHalfCeilingCopy().toString("yyyy-MM-dd HH:mm:ss"))
    //
    //    println(dateTime.dayOfYear().roundHalfCeilingCopy().toString("yyyy-MM-dd HH:mm:ss"))
    //
    //    println(dateTime.dayOfMonth().roundHalfCeilingCopy().toString("yyyy-MM-dd HH:mm:ss"))

    /** 实验一下deprecated关键字 */
    //    val at = new A
    //
    //    println(at.a(1, 2))
    //
    //    println(at.a(1, 2, true))
    //
    //    val a = 1
    //    val b= 1.0
    //
    //    (a, b) match {
    //      case (e1: Int, e2: Double) => {
    //        println("good")
    //      }
    //    }

    /** ClassTag学习 */
    //    import scala.reflect.ClassTag
    //    def mkArray[T: ClassTag](e: T*): Array[T] = Array(e:_*)
    //    mkArray(Array(1), Array(2), Array(3))
    //
    //
    //    class A
    //    class B(b: Int) extends A
    //    class C(c: Double) extends A
    //    class test[T >: A :ClassTag](){
    //
    //      def mkArray2(e: T): Array[T] = {
    //        val u: Array[T] = e match {
    //          case e1: B => Array(new B(1))
    //          case e2: C => Array(new C(2.0))
    //        }
    //        u
    //      }
    //
    //    }
    //
    //    println(new test[A]().mkArray2(new B(3)))


    //    println(df.schema.fields.map(_.dataType).mkString(","))
    //    df.rdd.foreach(row => {
    //      row.get(1) match {
    //        case _: Seq[StructType] => println("good")
    //        case _: Seq[Row] => println("bad")
    //
    //      }
    //    })


    //
    //    def getRdd(): RDD[String] = df.rdd.map(row => row.getAs[String](1))
    //
    //    def getDataFrame(): DataFrame = df.select("category")
    //
    //    class TwoTypeOutput[T <: Serializable : ClassTag]() {
    //      def generate(method: () => T): T = {
    //        type method1 = () => RDD[String]
    //        type method2 = () => DataFrame
    //
    //        method match {
    //          case mth1: method1 => mth1()
    //          case mth2: method2 => mth2()
    //          case _ => throw new Exception("不支持其他类型的函数")
    //        }
    //      }
    //    }
    //
    //    new TwoTypeOutput[DataFrame]().generate(getDataFrame).show(3)
    //    new TwoTypeOutput[RDD[String]]().generate(getRdd).take(3).foreach(println)


    import org.apache.spark.mllib.linalg.Matrix
    val a: Matrix = null


    new SimpleDateFormat("feafeafea")

//    def setA(name: String, age: Int) = {
//      println(name)
//      println(age)
//    }
//
//    setA("John", 18)
//
//    import org.apache.spark.sql.functions.{array, coalesce, col, collect_set, first, min}
//    val df = sqlc.createDataFrame(
//      Seq(
//        (1, 0, "A"),
//        (2, 2, "B"),
//        (1, 2, "C"),
//        (1, 2, "D"),
//        (2, 0, "E"),
//        (1, 0, "F")
//      )
//    ).toDF("col1", "col2", "col3")
//
//
//    df.groupBy(col("col1")).agg(min(col("col2")), collect_set(col("col3"))).show()
//
//    val dataFrame1 = sqlc.createDataFrame(
//      Seq(
//        ("Employee1", "salary100"),
//        ("Employee2", "salary50"),
//        ("Employee3", "salary200")
//      )).toDF("employee_A", "salary_A")
//
//
//    val dataFrame2 = sqlc.createDataFrame(
//      Seq(
//        ("Employee1", "salary100"),
//        ("Employee2", "salary50"),
//        ("Employee4", "salary600")
//      )).toDF("employee_B", "salary_B")
//
//    val joined = dataFrame1
//      .join(dataFrame2, col("employee_A") === col("employee_B"), "outer")
//      .select(
//        coalesce(col("employee_A"), col("employee_B")).as("employee"),
//        coalesce(col("salary_A"), col("salary_B")).as("salary")
//      )
//
//    joined.show()
















  }

}
