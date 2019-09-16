package com.self.core.learningKMeans

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

object learningKMeansMain extends myAPP {
  def createData() = {
    import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, Vectors}
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

    val rd = new java.util.Random(1123L)

    val cluster1 = List.fill(1000)(0.0)
      .map(d => (d, new DenseVector(Array.tabulate(5)(i => i + rd.nextGaussian))))
    val cluster2 = List.fill(1000)(1.0)
      .map(d => (d, new DenseVector(Array.tabulate(5)(i => - i + rd.nextGaussian))))
    val cluster3 = List.fill(1000)(2.0)
      .map(d => (d, new DenseVector(Array.tabulate(5)(_ => 3.0 + rd.nextGaussian))))
    val cluster4 = List.fill(1000)(3.0)
      .map(d => (d, new DenseVector(Array.tabulate(5)(_ => -3.0 + rd.nextGaussian))))

    val cluster = cluster1.union(cluster2).union(cluster3).union(cluster4)

    import org.apache.spark.mllib.linalg.Vector

    val rdd = sc.parallelize(cluster).map { case (d , v) => (d, v.asInstanceOf[Vector])}

//    val matrix = new DenseMatrix(2, 2, Array(3.0, -0.5, -0.5, 3.0))
//
//
//    val newRdd = rdd.mapValues(v => matrix.multiply(v))
//      .map{case (label, feature) => {
//        if(label == 0.0) {
//          Row.fromSeq(label +: feature.values)
//        }else{
//          val u = new DenseVector(feature.values
//            .zip(Array(4.0, 4.0)).map(d => d._1 + d._2))
//          Row.fromSeq(label +: u.values)
//        }
//      }
//      }

    val newRdd = rdd.map{
      case (label, u) =>
          Row.fromSeq(Array(label, u))
    }

    newRdd.cache()

    println(s"数据总计${newRdd.count()}条")
    import org.apache.spark.mllib.clustering.KMeans

    for(j <- 0 until 5) {
      val time1 = System.nanoTime()
      for (i <- 0 until 10) {
        KMeans.train(rdd.values, 5, 200, 1)
      }
      val endTime1 = System.nanoTime()
      println(s"重复10次训练，花费${(endTime1 - time1) / 1000}毫秒")


      val time2 = System.nanoTime()
      KMeans.train(rdd.values, 5, 200, 10)

      val endTime2 = System.nanoTime()
      println(s"每次同时训练10个模型，花费${(endTime2 - time2) / 1000}毫秒")
    }
    /** 测试效果 */

    /*
    数据总计4000条
    重复10次训练，       花费54678502毫秒
    每次同时训练10个模型，花费11454794毫秒
    重复10次训练，       花费36555154毫秒
    每次同时训练10个模型，花费5230959毫秒
    重复10次训练，       花费40251928毫秒
    每次同时训练10个模型，花费7440861毫秒
    重复10次训练，       花费37958866毫秒
    每次同时训练10个模型，花费5899895毫秒
    重复10次训练，        花费40952640毫秒
    每次同时训练10个模型， 花费6722452毫秒
     */













    //
//    val newDataFrame = sqlc.createDataFrame(newRdd, StructType(
//      Array(StructField("原类型", DoubleType), StructField("对应值", new VectorUDT))))
//
//
//    /** 输出 */
//    newDataFrame.cache()
//    newDataFrame.registerTempTable("数据源_DpO7ZL")
//    sqlc.cacheTable("数据源_DpO7ZL")
  }


  override def run(): Unit = {
    import org.apache.spark.mllib.clustering.KMeans








  }
}
