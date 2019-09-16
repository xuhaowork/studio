package com.self.core.polr.models

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


/**
  * editor: xuhao
  * date: 2018-05-05 10:30:00
  */

/**
  * 有序回归
  * ----
  * 模型全称是：
  * cumulative logistic model for ordinal response或
  * proportional odds logistic regression (POLR)
  * ----
  * 该算法利用了方程的一种等价形式求解，而不是直接基于似然函数。
  * ----
  *
  * @param levels
  * @param numIterations
  * @param stepSize
  * @param miniBatchFraction
  */

class Polr(levels: Int,
           numIterations: Int,
           stepSize: Double,
           miniBatchFraction: Double) extends Serializable {
  def this() = this(2, 200, 0.3, 0.3)

  def this(k: Int) = this(k, 200, 0.3, 0.1)

  def run(input: RDD[LabeledPoint]): PolrModel = {
    /**
      * 特征转化
      */
    val numFeatures = input.first().features.size
    val transformRdd = input.flatMap(transform(_, levels))

    transform(LabeledPoint(1.0, new DenseVector(Array(1.3, 1.0))), 3)

    val logitModel: LogisticRegressionModel = LogisticRegressionWithSGD.train(transformRdd,
      numIterations, 5.5, 1.0)
    import VectorImplicit.VectorLastImplicit
    val interceptLst = logitModel.weights.last(levels - 1).map(-_)
    import ArrayUtilsImplicit.ArrayCumsum
    val multiIntercept = interceptLst.cumsum

    import VectorImplicit.VectorDropImplicit
    val weights = logitModel.weights.dropRight(levels - 1)
    new PolrModel(multiIntercept, weights, numFeatures, levels)
  }

  private def transform(labeledPoint: LabeledPoint, levels: Int): Array[LabeledPoint] = {
    // k is the levels
    Array.tabulate(levels - 1)(k => {
      if (util.Try(labeledPoint.label).getOrElse(0.0) > k) {
        val newFeatures = new DenseVector(labeledPoint.features.toDense.values ++
          Array.tabulate(levels - 1)(i => if (i <= k) 1.0 else 0.0)
        )
        LabeledPoint(1.0, newFeatures)
      } else {
        val newFeatures = new DenseVector(labeledPoint.features.toDense.values ++
          Array.tabulate(levels - 1)(i => if (i <= k) 1.0 else 0.0)
        )
        LabeledPoint(0.0, newFeatures)
      }
    })
  }


}


object Polr {
  /**
    *
    * @param input             级别从小到大依次为0, 1, 2, ..., levels - 1
    * @param levels            级别数
    * @param numIterations     最大迭代次数
    * @param stepSize          梯度下降的步长
    * @param miniBatchFraction 迭代批次的比率
    * @param initialWeights    初始权重
    * @return
    */
  def train(
             input: RDD[LabeledPoint],
             levels: Int,
             numIterations: Int,
             stepSize: Double,
             miniBatchFraction: Double,
             initialWeights: Vector): Unit = {
  }

}
