package cn.datashoe.examples

import org.scalatest.FunSuite


/**
  * spark调用过程中的一些知识点
  */
class SparkEvoke extends FunSuite {
  /**
    * mapPartition中Iterator的迭代
    * ----
    * 利用Iterator直接定义一个新的Iterator
    * 1)不用转存为数组等结构, 防止数据过大栈溢出
    * 2)限制是不适合前后调用跨度特别大的mapPartition函数
    */
  test("iterator取差集") {
    import cn.datashoe.sparkBase.TestSparkAPP
    val a = TestSparkAPP.sc.parallelize(1 to 20, 2)

    def diff1(iter: Iterator[Int]): Iterator[Int] = {
      if (iter.isEmpty)
        Iterator[Int]()
      else {
        var value = iter.next()
        new Iterator[Int] {
          def hasNext: Boolean = iter.hasNext

          def next(): Int = {
            val lastOne = value
            value = iter.next()
            value - lastOne
          }
        }
      }
    }

    /*
    def diff2(iter: Iterator[Int]): Iterator[Int] = {
      val arr = iter.toArray
      arr.sliding(2).map {
        s =>
          s(1) - s(0)
      }
    }
    */

    /**
      * 结果解析
      * ----
      * 第1个分区
      * (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      * 结果
      * (1, 1, 1, 1, 1, 1, 1, 1, 1)
      *
      * 第2个分区
      * (11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
      * 结果
      * (1, 1, 1, 1, 1, 1, 1, 1, 1)
      */
    println("原数据")
    a.mapPartitionsWithIndex {
      case (partitionId, iter) =>
        Iterator((partitionId, iter.mkString(", ")))
    }.foreach(println)

    println("差分后数据")
    a.mapPartitions(diff1).mapPartitionsWithIndex {
      case (partitionId, iter) =>
        Iterator((partitionId, iter.mkString(", ")))
    }.foreach(println)


  }



}
