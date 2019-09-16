package com.self.core.BMJL.kmeansClustering

import com.self.core.baseApp.myAPP
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Created by DataShoe on 2018/1/15.
  */
object abKmeans extends myAPP{
  override def run(): Unit = {
//    // 初步实践
//    val path = "F://dataset/Wholesale customers data.csv"
//
//    val rdd = sc.textFile(path)
//    val rdd_head = rdd.first
//
//    val schema = rdd_head.split(",", 8).map(x => StructField(x, IntegerType))
//
//    val data = rdd.filter(_ != rdd_head)
//        .map(x => Vectors.dense(
//          x.split(",", 8)
//            .map(e => if(!e.equals("")) e.toDouble else 0.0)))
//
//    // 将数据拆分为训练集和预测集
//    val splitData = data.randomSplit(Array(0.6, 0.4), seed = 123L)
//    val train_data = splitData(0)
//    val test_data = splitData(1)
//
//    // 训练模型
//    val k = 5
//    val maxIterations = 200
//    val runs = 10
//
//    val model = KMeans.train(
//      train_data: RDD[Vector],
//      k: Int,
//      maxIterations: Int,
//      runs: Int)
//
//    // 评估训练结果
//    val mean_distance = test_data.map(v => {
//      val id = model.predict(v)
//      val center_vector = model.clusterCenters(id)
//      v.toArray.zip(center_vector.toArray).map{case (d1, d2) => (d1 - d2)*(d1 - d2)}.sum
//    }).mean()
//
//
//    val k_array = (2 until 10 by 1).map(k => {
//      val model = KMeans.train(
//        train_data,
//        k,
//        maxIterations,
//        runs)
//
//      // 评估训练结果
//       val mean_distance = test_data.map(v => {
//        val id = model.predict(v)
//        val center_vector = model.clusterCenters(id)
//        v.toArray.zip(center_vector.toArray)
//          .map{case (d1, d2) => (d1 - d2)*(d1 - d2)}
//          .sum
//      }).mean()
//      mean_distance
//    })
//    k_array.foreach(println)
//
//    // 异常检测，取100个异常值点
//    val new_model = KMeans.train(
//      train_data: RDD[Vector],
//      3: Int,
//      maxIterations: Int,
//      runs: Int)
//
//    val threshold = train_data.map(v => {
//      val id = model.predict(v)
//      val center_vector = model.clusterCenters(id)
//      v.toArray.zip(center_vector.toArray)
//        .map{case (d1, d2) => (d1 - d2)*(d1 - d2)}
//        .sum
//    }).top(100).last
//
//
//    test_data.map(v => {
//      val id = new_model.predict(v)
//      val center_vector = new_model.clusterCenters(id)
//      val distance = v.toArray.zip(center_vector.toArray)
//        .map{case (d1, d2) => (d1 - d2)*(d1 - d2)}
//        .sum
//      (distance, id)
//    }).filter(_._1 > threshold).foreach(println)
//
//
//    import org.apache.spark.mllib.classification.SVMModel


    // view的应用
//    val a = 0 until 100000
//    val sumAll = a.zip(100000 until 200000).flatMap(x => Array.tabulate(x._1)(i => x._2)).sum
//    println(sumAll) // not compile, Exception: Out of GC
//
//    val sumAllView = a.view.zip(100000 until 200000).flatMap(x => Array.tabulate(x._1)(i => x._2)).sum
//    println(sumAllView)

    println(math.log(math.exp(1.2)))

    val u = mutable.HashMap("" -> 1, "1" -> 2, -1L -> 3).remove("-1").getOrElse(mutable.HashMap("" -> 1, "1" -> 2, -1L -> 3))



  }
}
