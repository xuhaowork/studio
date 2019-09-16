package com.self.core.BMJL.ETL

import breeze.linalg
import breeze.linalg.{DenseVector, diag}
import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.DenseMatrix
import scala.collection.mutable.ArrayBuffer

/**
  * Created by DataShoe on 2018/1/9.
  */
object dataETL extends myAPP{
  override def run(): Unit = {

    import org.apache.spark.mllib.linalg.Vector
    val s: linalg.Vector[Double] = linalg.Vector(1, 2, 4.0)

    println(diag(s.toDenseVector))

    DenseMatrix.eye(10).transpose


    val umap = for(i <- 0 until 10) yield {(i, 1)}

    umap.toMap.foreach(println)











  }
}
