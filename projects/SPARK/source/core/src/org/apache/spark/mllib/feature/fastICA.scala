package org.apache.spark.mllib.feature

import breeze.linalg.{diag, DenseMatrix => BDM, DenseVector => BDV, max => brzMax, svd => brzSvd}
import breeze.numerics.{abs => brzAbs, sqrt => brzSqrt}
import org.apache.spark.mllib.feature.VectorTransformer
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, Matrix, Vector, Vectors}
import org.apache.spark.rdd.RDD
//import org.apache.spark.Logging
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel


/**
  * editor: xuhao
  * date: 2018-02-10 09:00:00
  */

/**
  * fastICA
  * ------------
  * An algorithm for Independent Component Analysis.
  * Which can be refer to :
  * [1]'A. Hyvarinen and E. Oja (2000) Independent Component
  * Analysis: Algorithms and Applications, Neural Networks, 13(4-5):411-430'
  * [2]'r/CRAN/web/packages/fastICA/fastICA.pdf'
  */


/** Generally speaking:
  * We assume that the data is X with dims of N*n,what we want is to find a
  * matrix A and S with dims of N*m, A*S = X. The latent data of S is component
  * ------------
  * of n variables which is nonGaussian.
  * First we need to white X to Z, assume that E(x*t(x)) = E*D*E, then
  * Z = `E {D}-1 E`.
  *
  * Second, we need to find a matrix A to transform Z to S which is the most
  * nonGaussian:
  *   1)The way to identify the optimum solution is projection pursue in which
  *   the nonGaussian is measured by a statistic -- neg-entropy;
  *   2)We approximate the neg-entropy by `E{{g(w'*x) - gaussian}^2}`;
  *   3)Under Kuhn-Tucker condition we can locate the optimum solution by fixed
  *   point -- E{xg(w'*x)} - beta*W = 0, beta = E{w*'Xg(w*'x)}, w* is the
  *   optimum solution.
  *   4)We can use Newton algorithm to locate the root of the equation --
  *   w := w - f(x)/f'(x)
  *   f'(x) = E{x*x'g'(w'*x)} - beta, approximate to E{g'(w'*x)} - beta
  *   we estimate the beta via assume w* is w in this iteration.
  *   5) So the final iteration equation is:
  *   w := E{xg(w'*x)} - E{g'(w'*x)}*w;
  *   w = w / ||w||.
  */


class fastICA(private var componetNums: Int, private var alpha: Double,
              private var threshold: Double, private var sampleFraction: Double,
              private var seed: Long, private var maxIterations: Double,
              private var whiteMatrixByPCA:
              (Boolean, Int)) extends Logging{

  def this() = this(2, 1.5, 1E-4, 1.0, {new java.util.Random()}.nextLong(),
    200, (false, 2))


  /** The independent component numbers. */
  def setComponetNums(componetNums: Int): this.type = {
    this.componetNums = componetNums
    this
  }

  def setAlpha(alpha: Double): this.type = {
    if(alpha <= 0.0)
      logError("Alpha should be positive number.")
    if(alpha < 1 || alpha > 2)
      logWarning("Alpha is suggested to be between 1.0 and 2.0")
    this.alpha = alpha
    this
  }

  def setThreshold(threshold: Double): this.type = {
    if(threshold > 0.1 || threshold < 1E-20)
      logWarning("Threshold is suggested to be between 0.1 and 1e-20.")
    this.threshold = threshold
    this
  }

  def setSampleFraction(sampleFraction: Double): this.type = {
    if(sampleFraction <= 0.0 || sampleFraction >= 1.0)
      logError("The sample fraction should be between 1 and 0.")
    this.sampleFraction = sampleFraction
    this
  }

  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  def setWhiteMatrixByPCA(num: Int): this.type = {
    this.whiteMatrixByPCA = (true, num)
    this
  }

  private def getPCANums(colNums: Int): Int = {
    if(this.whiteMatrixByPCA._1){
      val PCANums = this.whiteMatrixByPCA._2
      require(colNums >= PCANums && PCANums >= this.componetNums,
        "The numbers of columns and PCA components should not be less than " +
          "component numbers.")
      PCANums
    } else
      this.componetNums
  }


  private def checkNums(colNums: Long, rowNums: Long): Boolean =
    if(this.whiteMatrixByPCA._1)
      this.whiteMatrixByPCA._2 <= scala.math.min(colNums, rowNums)
    else
      componetNums <= scala.math.min(colNums, rowNums)

  /** Transform the matrix to a matrix with zero means. Used in whiteMatrix. */
  private def standardWithMean(matrix: RowMatrix)
  : RowMatrix = {
    val statisticMpdel = matrix.computeColumnSummaryStatistics()

    val mean = statisticMpdel.mean.toArray
    val sd = statisticMpdel.variance.toArray.map(math.sqrt)

    val meanBC = matrix.rows.context.broadcast(mean)
    val sdBC = matrix.rows.context.broadcast(sd)
    val result = matrix.rows.mapPartitions(iter =>
      iter.map(v => {
        val bv = v.toArray.zip(meanBC.value)
          .map{case (e, means) => e - means}
        bv.zip(sdBC.value).map{case (v, sd) => v / sd}
        Vectors.dense(bv)
      })
    )
    new RowMatrix(result)
  }


  /** Transform the matrix to a white matrix. */
  private def whiteMatrix(xMatrix: RowMatrix, svdNums: Int) = {
    val meanScaleMatrix = standardWithMean(xMatrix)
    val whiteMatrix: Matrix = meanScaleMatrix.computePrincipalComponents(svdNums)

    val zMatrix: RDD[Vector] = xMatrix.rows.map(vec => {
      whiteMatrix.transpose.multiply(new DenseVector(vec.toArray))
    })
    (new RowMatrix(zMatrix), whiteMatrix)
  }


  /** The main function of ICA. */
  def fit(rddVector: RDD[Vector]): ICAModel = {
    if (rddVector.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance" +
        " if its parent RDDs are also uncached.")
    }

    val xMatrix: RowMatrix = new RowMatrix(rddVector)
    val colNums = xMatrix.numCols()
    val rowNums = xMatrix.numRows()
    require(checkNums(colNums, rowNums), "The num of components " +
      "should be not bigger than colNums and rowNums.The max number here" +
      s" is ${math.min(colNums, rowNums)}. And also the the num of components" +
      s" should be no bigger than the num of principal components.")

    // White the matrix, x => z = K*x, x dimensions is n*1, z dimensions is p*1.
    val p: Int = getPCANums(xMatrix.numCols().toInt)
    val whiteModel = whiteMatrix(xMatrix: RowMatrix, p)
    // We define the PCA matrix as its' transpose to be in consistent with ICA matrix.
    val K: Matrix = whiteModel._2.transpose
    val zMatrix = whiteModel._1

    // Initial the matrix W, s.
    val rdn = new java.util.Random(this.seed)
    val m: Int = this.componetNums

    val initMatrix = orthogonalize(BDM.tabulate(m, p)((_, _) => rdn.nextDouble()))
    val initialWeight: Matrix = new DenseMatrix(initMatrix.rows, initMatrix.cols, initMatrix.data)

    var W_old = initialWeight.copy
    var W = initialWeight.copy

    var flag = true
    var i = 0

    // Iterations.
    val alphaN = this.alpha
    while(flag && i < maxIterations) {
      val WUpdate: RDD[Array[Double]] = zMatrix.rows.map(v => {
        val gwz = W.multiply(v).values.map(d => math.tanh(alphaN * d))
        val egwz = gwz.flatMap(d => v.toArray.map(_ * d))
        val dgwz = gwz.map(d => 1 - alphaN * d * d)
        val wdgwz = Array.tabulate(W.numCols * W.numRows)(i => {
          val rowId = i / W.numCols
          val colId = i % W.numCols
          W.transpose.apply(colId, rowId) * dgwz(rowId)
        })
        egwz.zip(wdgwz).map { case (e1, e2) => e1 - e2 }
      })

      /** Estimation of the expectation. */
      val expectation4WUpdate = EStep(WUpdate: RDD[Array[Double]], sampleFraction,
        seed, W.numCols * W.numRows)

      val newW = new BDM[Double](W.numRows, W.numCols, expectation4WUpdate, 0, W.numCols, true)
      val orthogonalMatrix = orthogonalize(newW)

      // judge
      W = new DenseMatrix(orthogonalMatrix.rows, orthogonalMatrix.cols, orthogonalMatrix.data)
      val multiMatrix = W_old.asBreeze.asInstanceOf[BDM[Double]] * orthogonalMatrix.t

      val v = brzMax(brzAbs(diag(multiMatrix)))
      if (v <= threshold) {
        flag = false
      } else {
        W_old = W.copy
      }

      i += 1
    }

    if(i >= maxIterations)
      logWarning("The param has not converged.")
    new ICAModel(K, W)
  }


  def EStep(WUpdate: RDD[Array[Double]], fraction: Double = 1.0, seed: Long, len: Int)
  : Array[Double] = {
    val WUpdateSample: RDD[Array[Double]] = WUpdate.sample(false, fraction, seed)
    val meanWUpdate = WUpdateSample
      .treeAggregate((0L, Array.fill(len)(0.0)))(
        (u, arr) => (u._1 + 1, u._2.zip(arr).map{case (e1, e2) => e1 + e2}),
        (u1, u2) => (u1._1 + u2._1, u1._2.zip(u2._2).map{case (e1, e2) => e1 + e2})
      )
    meanWUpdate._2.map(_ / meanWUpdate._1)
  }


  def orthogonalize(G: BDM[Double]): BDM[Double] = {
    /** test */
    println(G.data.mkString(","))
    val brzSvd.SVD(u, s, _) =
      try{
        brzSvd(G.t * G)
      }catch{
        case e: Exception => throw new Exception("svd Error.")
      }
    val recSqrtVector = BDV.fill(s.length)(1.0) :/ (brzSqrt(s) + 1e-5)
    val recSqrtMatrix = u * diag(recSqrtVector) * u.t
    G * recSqrtMatrix
  }

}


class ICAModel(val PCAMatrix: Matrix, val ICAMatrix: Matrix) extends VectorTransformer {
  override def transform(vector: Vector) = {
    ICAMatrix.multiply(PCAMatrix.asInstanceOf[DenseMatrix]).multiply(vector)
  }

}