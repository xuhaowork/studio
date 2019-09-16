package cn.datashoe.examples

import org.scalatest.FunSuite

/**
  * spark一些源码的解读和API调用
  */
class SparkAPISuite extends FunSuite {
  test("spark window函数") {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.{SparkConf, SparkContext}
    import org.joda.time.DateTime

    import cn.datashoe.sparkBase.TestSparkAPP.sqlc
/*    val conf = new SparkConf().setAppName("Window Functions").setMaster("local")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sqlc = sparkSession.sqlContext*/

    import org.apache.spark.sql.functions._

    val l = (1997, 1) :: (1997, 4) :: (1998, 2) :: (1998, 3) :: (1999, 9) :: Nil

    val df = sqlc.createDataFrame(l).toDF("k", "v").repartition(col("v"))

    println("原数据")
    df.show()

    // 原始分区
    // [1997,4]
    // [1998,2], [1997,1]
    //
    // [1999,9], [1998,3]
    //
    //    df.foreachPartition {
    //      a =>
    //        println(a.mkString(", "))
    //    }
    //
    //
    val df1 = df.repartition(col("k"))
    //
    //    val pairRdd = sc.parallelize(Seq((1, 2), (3, 4)))


    //    +----+---+
    //    |   k|  v|
    //    +----+---+
    //    |1997|  1|
    //    |1997|  4|
    //    |1998|  2|
    //    |1998|  3|
    //    |1999|  9|
    //    +----+---+

    //    val w = Window.partitionBy("")
    //    val df1 = df.withColumn("No", row_number().over(w))

    //    sum(x^2) - count * avg(x)^2
    //    count sum(x), sum(x ^ 2)
    //    sum(x ^ 2) - sum(x) ^ 2 / count
    //    1, x, x^2
    //
    //    udaf


    //    df1.rdd.mapPartitionsWithIndex {
    //      case (i, rows) =>
    //        Array((i, rows.mkString(", "))).toIterator
    //    }.foreach(println)

    println(new DateTime(2004, 1, 1, 0, 0, 0).getMillis)


    // 一班 姓名 乘积

    //    percent_rank().over(Window.orderBy("姓名").partitionBy("班级"))
    //    row_number().over(Window.orderBy("姓名").partitionBy("班级"))
    //    avg("").over()
    //
    //
    //    val u1 = Array.range(0, 1 << 31).map {
    //      i =>
    //        (i, "A")
    //    }
    //
    //    val u2 = Array.range(0, 1 << 31).map {
    //      i =>
    //
    //    }


    //    +----+---+---+------------------+
    //    |   k|  v| No|               row|
    //    +----+---+---+------------------+
    //    |1997|  1|  1|               1.0|
    //    |1997|  4|  2|               2.5|
    //    |1998|  2|  3|2.3333333333333335|
    //    |1998|  3|  4|               3.0|
    //    |1999|  9|  5| 4.666666666666667|
    //    +----+---+---+------------------+


    //    val rowW = w.rowsBetween(-2, 0)
    //    val rangeW = w.rangeBetween(-1, 0)
    //
    //    val res = df1.withColumn("row", avg($"v").over(rowW))
    //    res.show()
    ////      .withColumn("range", avg($"v").over(rangeW)).show
    //    sc.stop()

  }


  /**
    * spark rdd排序源码解读
    */
  test("排序源码解读") {
    import cn.datashoe.sparkBase.TestSparkAPP
    val a = TestSparkAPP.sc.parallelize(1 to 20, 2)
    a.sortBy(s => -s).take(10).foreach(println)

    // sortBy源于sortByKey
    // sortByKey会经过如下步骤:
    // 1)创建一个分区器，将数据分为不同的分区
    // val part = new RangePartitioner(numPartitions, self, ascending)
    // 2)
    // new ShuffledRDD[K, V, V](self, part)
    //      .setKeyOrdering(if (ascending) ordering else ordering.reverse)




  }


  test("top源码解读") {
    import cn.datashoe.sparkBase.TestSparkAPP
    val a = TestSparkAPP.sc.parallelize(1 to 20, 2)
    a.top(10)


  }


  test("goole guava排序器的使用") {
    /** 注意使用了 [[scala.collection.JavaConverters]] 该类的使用会在**/
    import scala.collection.JavaConverters._
    import scala.collection.JavaConverters.asJavaIteratorConverter

    import com.google.common.collect.{Ordering => GuavaOrdering}
    // 注意到takeOrdered使用了com.google.common.collect.Ordering的比较器
    object Utils {

      /**
        * Returns the first K elements from the input as defined by the specified implicit Ordering[T]
        * and maintains the ordering.
        */
      def takeOrdered[T](input: Iterator[T], num: Int)(implicit ord: Ordering[T]): Iterator[T] = {
        val ordering = new GuavaOrdering[T] {
          override def compare(l: T, r: T): Int = ord.compare(l, r)
        }
        ordering.leastOf(input.asJava, num).iterator.asScala
      }
    }
  }

  /** spark SQL explode和flatMap */
  test("spark SQL explode和flatMap") {
    import cn.datashoe.sparkBase.TestSparkAPP
    import TestSparkAPP.sqlc.implicits._
    val df = TestSparkAPP.sc.parallelize(1 to 20, 2).map(Tuple1.apply).toDF("value")

    import org.apache.spark.sql.functions._

    import org.apache.spark.sql.functions.udf
    import org.apache.spark.sql.functions.col

    val uu = udf {
      value: Double =>
        Array(value / 5, value % 5)
    }

    df.select(col("value"), explode(uu(col("value")))/*.as(Array("a", "b"))*/).show()

    df.select(col("value"), explode_outer(uu(col("value")))/*.as(Array("a", "b"))*/).show()

    df.select(col("value"), posexplode(uu(col("value")))/*.as(Array("a", "b"))*/).show()

    // 结论
    // explode是类似于flatMap的操作

  }

}
