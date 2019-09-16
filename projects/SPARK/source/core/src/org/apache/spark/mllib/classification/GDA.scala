package org.apache.spark.mllib.classification


import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.BLAS.{axpy, gemm, scal, syr, dot}
import org.apache.spark.mllib.linalg.{DenseMatrix, Vector, Vectors}
import breeze.linalg.inv
import breeze.linalg.{DenseMatrix => BDM}
import scala.math


/**
  * editor: xuhao
  * date: 2018-02-28 09:00:00
  */

/**
  * GDA
  * ------------
  * 1.Some params like the prediction format；
  * 2.Estimate the params in Bernoulli-Gaussian Model；
  *   ----
  *  Like most documents, We assume that distributions of x|y are homoscedastic;
  * 3.Output the GDA models which has a function of prediction.
  * ------------
  * 1.In current, the model does not support sparse vectors.
  * 2.Not support saving of the model which maybe used to predict another
  * similar data.
  */


class GDA(private var predictFormat: String) extends Logging{
  def this() = this("possibility")


  /** The predict format. */
  def setPredictFormat(predictFormat: String): this.type = {
    this.predictFormat = predictFormat
    this
  }

  /** The main algorithm function. */
  def run(input: RDD[LabeledPoint]): GDAModel = {
    if (input.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance "
        + "if its parent RDDs are also uncached.")
    }

    val data = input.mapPartitions(
      iter => iter.map{
        case labelPoint => {
          val count = 1
          val labels = labelPoint.label
          val features = labelPoint.features
          (count, labels, features)
        }
      })

    val size = input.take(1).head.features.size

    /** m, fi, mu0, mu1, sigma*/
    val zeroValue = (0, 0.0, Vectors.zeros(size), Vectors.zeros(size),
      DenseMatrix.zeros(size, size))

    val (m: Int, fi: Double, mu0: Vector, mu1: Vector, sigma: DenseMatrix) =
      data.treeAggregate(zeroValue)(seqOp = (u, v) => {
        val count = v._1
        val labels = v._2
        val features = v._3

        val m = u._1 + count
        val fi = u._2 + labels
        val mu0 = u._3
        val mu1 = u._4
        val sigma = u._5

        axpy(1 - labels, features, mu0)
        axpy(labels, features, mu1)
        syr(1.0, features, sigma)
        (m, fi, mu0, mu1, sigma)
      },
      combOp = (u1, u2) => {
        val (m1, m2) = (u1._1, u2._1)
        val (fi1, fi2) = (u1._2, u2._2)

        val (mu0_1, mu0_2) = (u1._3, u2._3)
        axpy(1.0, mu0_1, mu0_2)
        val (mu1_1, mu1_2) = (u1._4, u2._4)
        axpy(1.0, mu1_1, mu1_2)
        val (sigma1, sigma2) = (u1._5, u2._5)
        gemm(1.0, sigma1, DenseMatrix.eye(size), 1.0, sigma2)
        (m1 + m2, fi1 + fi2, mu0_2, mu1_2,sigma2)
      })

    gemm(0.0, DenseMatrix.zeros(size, size), DenseMatrix.zeros(size, size), 1.0 / m, sigma)
    scal(1.0 / (m - fi), mu0)
    scal(1.0 / fi, mu1)
    new GDAModel(fi / m, mu0, mu1, sigma, this.predictFormat)
  }



}

class GDAModel(val fi: Double, val mu0: Vector, val mu1: Vector, val sigma: DenseMatrix,
               val predictFormat: String)
  extends ClassificationModel with Serializable{
  override def predict(testData: Vector): Double = {
    val testDataToMu0 = testData.copy
    val testDataToMu1 = testData.copy
    axpy(-1.0, mu0, testDataToMu0)
    axpy(-1.0, mu1, testDataToMu1)

    val invSigmaBreeze: BDM[Double] = inv.apply(sigma.toBreeze.toDenseMatrix)
    val invSigma = new DenseMatrix(invSigmaBreeze.rows, invSigmaBreeze.cols,
      invSigmaBreeze.data)

    val normMu0 = -0.5 * dot(testDataToMu0, invSigma.multiply(testDataToMu0))
    val normMu1 = 0.5 * dot(testDataToMu1, invSigma.multiply(testDataToMu1))

    val probability: Double = if(fi <= 0.0){
      0.0
    }else if(fi >= 1.0){
      1.0
    }else{
      val thetaX = normMu0 + normMu1 + util.Try(math.log(1 / fi - 1)).getOrElse(0.0)
      1 / (1.0 + math.exp(thetaX))
    }

    predictFormat match {
      case "possibility" => probability
      case "classification" => if(probability < 0.5) 0.0 else 1.0
    }
  }

  override def predict(testData: RDD[Vector]): RDD[Double] = {
    testData.mapPartitions(iter => iter
      .map(v => predict(v)))
  }

}

