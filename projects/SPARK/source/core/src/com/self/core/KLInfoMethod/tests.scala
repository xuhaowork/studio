//package com.self.core.KLInfoMethod
//
//import com.zzjz.deepinsight.basic.BaseMain
//import org.apache.spark.TaskContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.functions.{col, lit, monotonicallyIncreasingId, sum, udf}
//import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
//
//import scala.reflect.ClassTag
//
//object tests extends BaseMain{
//  override def run(): Unit = {
//
////    val jd = new java.util.Random(1123L)
////
////    val move1 = udf((id: Int, moveLength: Int) => {
////      id - moveLength
////    })
////
////    val move2 = udf((id: Int) => {
////      id - 2
////    })
////
////    var i = 0
////    var time1 = 0L
////    var time2 = 0L
////
////    val data = sqlc.createDataFrame(Array.fill(100000)(Tuple1(jd.nextLong()))).toDF("id")
////    data.cache()
////    println(data.count())
////
////    while(i < 20) {
////      val startTime1 = System.nanoTime              //系统纳米时间
////
////      data.select(sum(move1(col("id"), lit(2)))).head().get(0)
////
////      val endTime1=System.nanoTime
////      val delta1= endTime1 - startTime1
////      time1 += delta1/1000000
////
////      val startTime2 = System.nanoTime              //系统纳米时间
////
////      data.select(sum(move2(col("id")))).head().get(0)
////
////      val endTime2=System.nanoTime
////      val delta2= endTime2 - startTime2
////      time2 += delta2/1000000
////      i += 1
////    }
////
////    println("time1", time1)
////    println("time2", time2)
////
////    (time1,67959)
////    (time2,69031)
//
//    import org.apache.spark.sql.functions.sparkPartitionId
//
//    import org.apache.spark.sql.functions.monotonicallyIncreasingId
//
//    val rdd: RDD[Int] = sc.parallelize(Array.range(0, 200), 5)
////      .map(i => Row.apply(i))
//
//
//
//    val lth = rdd.partitions.length
//    val u = sc.runJob(rdd, (it: Iterator[Int]) => it.take(5).toArray, Range(lth - 2, lth))
//
//    u.foreach(arr =>
//      println(arr.mkString(","))
//    )
//
//
//
//
//
////    val df = sqlc.createDataFrame(rdd, StructType(Array(StructField("id", IntegerType))))
////      .withColumn("partition", sparkPartitionId())
////
////    val partitionNumMap = df.groupBy(col("partition")).count().as("count").rdd.map(row =>
////      (row.getAs[Int]("partition"), row.getAs[Long]("count"))).collectAsMap()
//
//
//
//
//
//  }
//}
