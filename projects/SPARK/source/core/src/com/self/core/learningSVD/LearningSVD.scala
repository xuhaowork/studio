package com.self.core.learningSVD

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector, Vectors}
import org.apache.spark.sql.Row


/**
  * Created by DataShoe on 2018/1/9.
  */
object LearningSVD extends myAPP{
  override def run(): Unit = {
//    val data =Array(
//      Vectors.dense(4.0 , 2.0 , 3.0),
//      Vectors.dense(5.0 , 6.0 , 1.0))
//
//    // Array[Vector]转换成DataFrame
//    val df = sqlc.createDataFrame(data.map(Tuple1.apply)).toDF("features")
//
//    df.show()
//
//
//    // DataFrame转换成RDD
//    val df_To_rdd=df.select("features").map { case Row(v: Vector) => v}
//
//
//
//    // RDD转换成矩阵
//    // 矩阵的每一行分布式存储
//    val mat: RowMatrix = new RowMatrix(df_To_rdd)
//
//
//    mat.computeSVD(2)
//
//    // 奇异值分解
//    // def computeSVD(k: Int,computeU: Boolean = false,rCond: Double = 1e-9)
//    //k：取top k个奇异值
//    //computeU：是否计算矩阵U
//    //rCond：小于1.0E-9d的奇异值会被抛弃
//    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(2, true)
//    // s奇异值向量
//    println(svd.s)
//    //[9.175746009338516,2.608770816324863]
//
//
//
//
//
//
//
//
//
//
//
//
//    // U右奇异矩阵
//    svd.U.rows.foreach(println)
//    // [-0.5355281357229256,0.8445173863510019]
//    // [-0.8445173863510022,-0.5355281357229257]
//    // V左奇异矩阵
//    println(svd.V)
//    // -0.6936438157910113  0.26848999628726217
//    // -0.6689549365582719  -0.5842345491209884
//    // -0.2671283393225135  0.7658871414947904
//
//







  }
}
