package org.apache.spark.mllib.classification

/**
  * probit回归的核心运算API
  */

/**
  * editor: xuhao
  * date: 2018-05-15 10:30:00
  */

import breeze.stats.distributions.Gaussian
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.classification.impl.GLMClassificationModel
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.BLAS.{axpy, dot}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.mllib.util.ToolsForMNP._
import org.apache.spark.mllib.util.{DataValidators, Saveable}
import org.apache.spark.rdd.RDD

class ProbitGradient(val numClasses: Int, var diff: Double, var smoothSampleTimes: Int) extends Gradient {
  //  require(numClasses == 2, "目前只支持二分类")

  def this() = this(2, 1E-5, 100)

  def this(numClass: Int) = this(numClass, 1E-5, 100)

  def this(numClass: Int, diff: Double) = this(numClass, diff, 100)

  def setDiff(newDiff: Double): this.type = {
    this.diff = newDiff
    this
  }

  def setSmoothSampleTimes(newSmoothSampleTimes: Int): this.type = {
    this.smoothSampleTimes = newSmoothSampleTimes
    this
  }

  override def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    val gradient = Vectors.zeros(weights.size)
    val loss = compute(data, label, weights, gradient)
    (gradient, loss)
  }

  override def compute(data: linalg.Vector,
                       label: Double,
                       weights: linalg.Vector,
                       cumGradient: linalg.Vector): Double = {
    val dataSize = data.size

    numClasses match {
      case 2 =>
        require(weights.size % dataSize == 0 && numClasses == weights.size / dataSize + 1)

        val q = 2 * label - 1.0
        val margin = dot(data, weights)
        val qMargin: Double = q * margin
        val gaussian = new Gaussian(0.0, 1.0)
        val multiplier = -q * gaussian.pdf(qMargin) / (gaussian.cdf(qMargin) + 1E-4)

        axpy(multiplier, data, cumGradient)

        val cdfMargin = gaussian.cdf(margin)
        -label*scala.math.log(cdfMargin + 1E-4) - (1 - label)*scala.math.log(1 - cdfMargin + 1E-4)

      case num: Int =>
        // 数值求导
        val oldLoss: Double = getNegativeLogLikelihood(data: linalg.Vector,
          label: Double,
          weights: linalg.Vector,
          num: Int)
        val gradient =
          for (i <- 0 until weights.size) yield {
            val denseWeights = weights.copy.toDense
            val addTerm = scala.math.max(denseWeights.values(i), 1.0) * diff
            denseWeights.values(i) = denseWeights.values(i) + addTerm
            val newLoss = getNegativeLogLikelihood(data: linalg.Vector,
              label: Double,
              denseWeights: linalg.Vector,
              num: Int,
              smoothSampleTimes: Int
            )
            (newLoss - oldLoss) / addTerm
          }
        axpy(1.0, new linalg.DenseVector(gradient.toArray), cumGradient)
        oldLoss
    }

  }
}


class ProbitRegressionWithSGD private[mllib](
                                              private var stepSize: Double,
                                              private var numIterations: Int,
                                              private var regParam: Double,
                                              private var miniBatchFraction: Double
                                            )
  extends GeneralizedLinearAlgorithm[ProbitRegressionModel] with Serializable {

  private var numClasses: Int = 2

  this.setFeatureScaling(false)

  def setNumClasses(numClasses: Int): this.type = {
    require(numClasses > 1, "分类数需要大于1")
    this.numClasses = numClasses
    numOfLinearPredictor = numClasses - 1
    if (numClasses > 2) {
      this.numClasses = numClasses
      optimizer.setGradient(new ProbitGradient(numClasses))
    } else {
      this.setFeatureScaling(true)
    }
    this
  }

  override val optimizer: GradientDescent = {
    println("optimizer:", numClasses)
    new GradientDescent(new ProbitGradient(numClasses), new SquaredL2Updater)
      .setStepSize(stepSize)
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setMiniBatchFraction(miniBatchFraction)
  }

  override protected val validators = List(multiLabelValidator)

  private def multiLabelValidator: RDD[LabeledPoint] => Boolean = { _ =>
    true
  }

  override protected def createModel(weights: Vector, intercept: Double): ProbitRegressionModel = {
    new ProbitRegressionModel(weights, intercept, numFeatures, numClasses)
  }
}


class ProbitRegressionWithLBFGS(private var numClasses: Int) extends GeneralizedLinearAlgorithm[ProbitRegressionModel]
  with Serializable {
  def this() = this(2)

  def setNumClasses(numClasses: Int): this.type = {
    require(numClasses > 1)
    this.numClasses = numClasses
    numOfLinearPredictor = numClasses - 1
    if (numClasses > 2) {
      optimizer.setGradient(new ProbitGradient(numClasses))
    } else {
      this.setFeatureScaling(true)
    }
    this
  }

  this.setFeatureScaling(false)

  override val optimizer = new LBFGS(new ProbitGradient(numClasses), new SquaredL2Updater)

  override protected val validators = List(multiLabelValidator)

  private def multiLabelValidator: RDD[LabeledPoint] => Boolean = { data =>
    if (numOfLinearPredictor > 1) {
      DataValidators.multiLabelValidator(numOfLinearPredictor + 1)(data)
    } else {
      DataValidators.binaryLabelValidator(data)
    }
  }

  override protected def createModel(weights: Vector, intercept: Double)
  : ProbitRegressionModel = {
    new ProbitRegressionModel(weights, intercept, numFeatures, numClasses)
  }
}


class ProbitRegressionModel(
                             override val weights: Vector,
                             override val intercept: Double,
                             val numFeatures: Int,
                             val numClasses: Int)
  extends GeneralizedLinearModel(weights, intercept) with ClassificationModel with Serializable
    with Saveable with PMMLExportable {

  def this(weights: Vector, intercept: Double) = this(weights, intercept, weights.size, 2)

  private var threshold: Option[Double] = Some(0.5)

  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  def getThreshold: Option[Double] = threshold

  def clearThreshold(): this.type = {
    threshold = None
    this
  }

  override protected def predictPoint(
                                       dataMatrix: Vector,
                                       weightMatrix: Vector,
                                       intercept: Double): Double = {
    require(dataMatrix.size == numFeatures)

    // If dataMatrix and weightMatrix have the same dimension, it's binary logistic regression.
    if (numClasses == 2) {
      val score = new Gaussian(0.0, 1.0).cdf(dot(weightMatrix, dataMatrix) + intercept)
      threshold match {
        case Some(t) => if (score > t) 1.0 else 0.0
        case None => score
      }
    } else {
      Array.range(0, numClasses).map(x => (x,
        getNegativeLogLikelihood(dataMatrix: linalg.Vector,
          x.toDouble,
          weightMatrix: linalg.Vector,
          numClasses: Int))).minBy(_._2)._1.toDouble
    }
  }

  @Since("1.3.0")
  override def save(sc: SparkContext, path: String): Unit = {
    GLMClassificationModel.SaveLoadV1_0.save(sc, path, this.getClass.getName,
      numFeatures, numClasses, weights, intercept, threshold)
  }

  override protected def formatVersion: String = "1.0"

  override def toString: String = {
    s"${super.toString}, numClasses = $numClasses, threshold = ${threshold.getOrElse("None")}"
  }
}

object Probit {
  /**
    *
    * @param input             输入的数据
    * @param numIterations     梯度下降的最大迭代次数
    * @param stepSize          梯度下降需要的学习率
    * @param miniBatchFraction SGD中需要的初始权重
    * @param initialWeights    初始值，需要和对应的长度一致
    * @param addIntercept      是否加入截距项
    * @return 得到的ProbitRegressionModel
    */
  def trainWithSGD(
                    input: RDD[LabeledPoint],
                    numClasses: Int,
                    numIterations: Int,
                    stepSize: Double,
                    miniBatchFraction: Double,
                    initialWeights: Vector,
                    addIntercept: Boolean
                  ): ProbitRegressionModel = {
    new ProbitRegressionWithSGD(stepSize, numIterations, 0.0, miniBatchFraction)
      .setIntercept(addIntercept).setNumClasses(numClasses)
      .run(input, initialWeights)
  }


  /**
    *
    * @param input             输入的数据
    * @param numIterations     梯度下降的最大迭代次数
    * @param stepSize          梯度下降需要的学习率
    * @param miniBatchFraction SGD中需要的初始权重
    * @return 得到的ProbitRegressionModel
    */
  def trainWithSGD(
                    input: RDD[LabeledPoint],
                    numClasses: Int,
                    numIterations: Int,
                    stepSize: Double,
                    miniBatchFraction: Double,
                    intercept: Boolean): ProbitRegressionModel = {
    val featureNums = input.first().features.size
    val rd = new java.util.Random(123L)
    var size = featureNums * numClasses + numClasses * (numClasses - 1) / 2
    if (numClasses > 2) {
      if(numClasses > 2)
        require(!intercept, "多分类不支持截距项")
      val initialWeights =
        if (intercept) {
          size += (if (numClasses == 2) 1 else numClasses)
          val arr = Array.fill[Double](size)(0.0)
          for (i <- 0 until numClasses * (numClasses - 1) / 2) {
            arr(size - 1 - i) = rd.nextDouble()
          }
          new linalg.DenseVector(arr)
        } else {
          val arr = Array.fill[Double](size)(0.0)
          for (i <- 0 until numClasses * (numClasses - 1) / 2) {
            arr(size - 1 - i) = rd.nextDouble()
          }
          new linalg.DenseVector(arr)
        }

      trainWithSGD(
        input: RDD[LabeledPoint],
        numClasses: Int,
        numIterations: Int,
        stepSize: Double,
        miniBatchFraction: Double,
        initialWeights,
        intercept
      )
    } else {
      val initialWeights = Vectors.zeros(featureNums)
      new ProbitRegressionWithSGD(stepSize, numIterations, 0.0, miniBatchFraction)
        .setIntercept(intercept).setFeatureScaling(false)
        .run(input, initialWeights)
    }
  }


  def trainWithSGD(
                    input: RDD[LabeledPoint],
                    numClasses: Int,
                    numIterations: Int,
                    stepSize: Double): ProbitRegressionModel = {
    trainWithSGD(input, numClasses: Int, numIterations, stepSize, 1.0, true)
  }

  def trainWithSGD(
                    input: RDD[LabeledPoint],
                    numClasses: Int,
                    numIterations: Int
                  ): ProbitRegressionModel = {
    trainWithSGD(input, numClasses: Int, numIterations, 1.0)
  }


  def trainWithLBFGS(
                      input: RDD[LabeledPoint],
                      numClasses: Int,
                      addIntercept: Boolean
                    ): ProbitRegressionModel = {
    if(numClasses > 2)
      require(!addIntercept, "多分类不支持截距项")
    new ProbitRegressionWithLBFGS(numClasses)
      .setIntercept(addIntercept)
      .setValidateData(false)
      .run(input)
  }

  def trainWithLBFGS(input: RDD[LabeledPoint], numClasses: Int): ProbitRegressionModel = {
    new ProbitRegressionWithLBFGS(numClasses)
      .setIntercept(true)
      .run(input)
  }


}


