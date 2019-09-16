package com.self.core.ART1NeuralNet.tests

import breeze.linalg.DenseVector
import com.self.core.baseApp.myAPP
import com.self.core.ART1NeuralNet.model.ARTModel
import org.apache.spark.rdd.RDD


object ART1NetTest extends myAPP {
  val trainData = Seq(
    // 类型1 第三列为黑其余全为白
    new DenseVector[Double](Array(0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0).map(_.toDouble)),
    // 类型2 第三列和第三行为黑其余全为白
    new DenseVector[Double](Array(0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0).map(_.toDouble)),
    // 类型3 第三列和第三行加一条对角线全为黑其余全为白
    new DenseVector[Double](Array(0, 0, 1, 0, 1, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 0, 0, 1, 0, 1, 0, 0).map(_.toDouble)),
    // 类型4 第三列第三行加两条对角线全为黑其余全为白
    new DenseVector[Double](Array(1, 0, 1, 0, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1).map(_.toDouble))
  )


  override def run(): Unit = {
    println("this is a main file")

    val rho: Double = 0.9 // 0.1
    val numFeatures = 25

    val model2 = ARTModel.init(rho, numFeatures)

    trainData.foreach {
      vec =>
        model2.learn(vec)
    }

    model2.knowledge.foreach(println)


    var model = ARTModel.init(rho, numFeatures, 200)

    val rdd: RDD[DenseVector[Double]] = sc.parallelize(trainData, 4)
    if (rdd.partitions.length > 100) {
      throw new Exception("ART1神经网络是一种online learning式的学习算法, 它实质上不是以分布式的方式运行的而是逐个分区运行的, 您的分区数超过100个, 可能会严重拖慢速度")
    }

    val numForeachPartition = rdd.mapPartitionsWithIndex {
        case (index, iter: Iterator[DenseVector[Double]]) => Iterator((index, iter.size.toLong))
      }.filter(_._2 > 0)
      .collect()

    model.knowledge.foreach(println)
    numForeachPartition.foreach {
      case (partitionId, _) =>
        val res = rdd.sparkContext.runJob(
          rdd,
          (iter: Iterator[DenseVector[Double]]) => {
            iter.foreach {
              vec =>
                println(s"在分区${partitionId}前, 知识为${model.knowledge.mkString(",")}")
                println(s"分区中的数据为：${vec.data.mkString(",")}")
                model.learn(vec)
            }
            model
          },
          Seq(partitionId)
        )
        model = res.head
        println(s"经过分区${partitionId}后, 知识为${model.knowledge.mkString(",")}")
    }


//    numForeachPartition.foreach {
//      case (partitionId, _) =>
//        val res = rdd.sparkContext.runJob(
//          rdd,
//          (iter: Iterator[DenseVector[Double]]) => {
//            iter.foreach {
//              vec =>
//                model.learn(vec)
//            }
//            model
//          },
//          Seq(partitionId)
//        )
//        println(s"经过分区${partitionId}后, 知识为${model.knowledge.mkString(",")}")
//    }
//    sc.runJob(rdd, (iter: Iterator[DenseVector[Double]]) => s + 1, Seq(0))





//
//    model.knowledge.foreach(println)
//
//    println("-" * 80)




  }
}
