package com.self.core.fastICA

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.fastICA
import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD

object ICATest extends myAPP{
  override def run(): Unit = {

    val rdn = new java.util.Random(123L)
    val rdd: RDD[linalg.Vector] = sc.parallelize(List.fill(10)(Array.range(1, 4)
      .map(_.toDouble + rdn.nextDouble()))).map(Vectors.dense)

    // step1. white the matrix.
    val fastICAObj = new fastICA().setComponetNums(3).setWhiteMatrixByPCA(3)
    val whiteModel = fastICAObj.fit(rdd).transform(rdd)
    whiteModel.foreach(println)

  }
}
