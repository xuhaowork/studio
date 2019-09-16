package com.self.core.elkkmeans

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary


/**
  * Created by DataShoe on 2018/1/26.
  */
object elkKmeans extends myAPP{
  override def run(): Unit = {
//    import org.apache.spark.mllib.clustering.mllib.ElkKmeans
////    ElkKmeans.printk()
////    import org.apache.spark.mllib.linalg.{Vector, Vectors}
////    val vs = Vectors.sparse(10, Array(0, 1, 2, 9), Array(9, 5, 2, 7))
////    println(vs(2))
////
////    import org.apache.spark.mllib.stat.Statistics
//    val rdd = sc.parallelize(List(Vectors.dense(Array(1.0, 2.0, 5.0)),
//  Vectors.dense(Array(11.0, 22.0, 5.0)),
//  Vectors.dense(Array(31.0, 2.0, 15.0)),
//  Vectors.dense(Array(12.0, 2.10, 5.80)),
//  Vectors.dense(Array(19.0, 2.20, 5.0)),
//  Vectors.dense(Array(12.0, 2.0, 5.30)),
//  Vectors.dense(Array(1.0, 24.0, 5.0)),
//  Vectors.dense(Array(31.0, 12.0, 5.0)),
//  Vectors.dense(Array(1.0, 21.0, 5.0)),
//  Vectors.dense(Array(12.0, 12.0, 5.20)),
//  Vectors.dense(Array(81.0, 2.0, 5.0)),
//  Vectors.dense(Array(1.0, 72.0, 5.0)),
//  Vectors.dense(Array(1.0, 2.0, 58.0)),
//  Vectors.dense(Array(1.0, 27.0, 5.0)),
//  Vectors.dense(Array(18.0, 2.0, 5.0)),
//  Vectors.dense(Array(1.0, 28.0, 5.0)),
//  Vectors.dense(Array(1.0, 22.0, 59.0)),
//  Vectors.dense(Array(16.0, 21.0, 5.0))))
//
//    rdd.cache()
//    val model = new ElkKmeans().run(rdd)
//    model.clusterCenters.foreach(println)
//
//    import org.apache.spark.ml.classification.LogisticRegression
//
//    import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

    println(8000/(1/0.11 - 1))

    import org.apache.spark.sql.functions.udf
    import org.apache.spark.mllib.clustering.KMeans




  }
}
