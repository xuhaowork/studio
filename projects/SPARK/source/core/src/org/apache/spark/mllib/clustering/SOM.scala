package org.apache.spark.mllib.clustering

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.BLAS.{axpy, dot, scal}
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.util.Loader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom
import scala.collection.mutable.{Map => muMap}

/**
  * SOM算法
  * ----
  * 自组织映射神经网络 --Self Organizing Map
  * 是一种无监督学习算法，最终的输出结果是带有拓扑结构的聚类，因此划归入聚类算法。
  * 参考文献： @see The element of statistical learning 14.4, 这里实现的是批处理模式
  * ----
  * 虽然叫做自组织神经网络但，由于其本质上是一种无监督学习的聚类算法，没有误差的反向传播等机制，
  * 算法本质上的逻辑更接近于KMeans
  * ----
  * 改进脚本实现的是批处理版本：
  * 1）相对于online版本没有学习率递减和邻域递减
  * 2）终止条件是神经元(‘聚类’)中心不再发生变化
  * 3）邻域函数目前是高斯函数
  * 4）神经元之间的拓扑结构可以是一维或二维，二维结构可以是矩形或正六边形。
  * 拓扑结构决定了邻域，从而决定了神经元向量的更新。
  */
class SOM(val somGrid: SOMGrid, val numFeatures: Int, val maxIterations: Int, val epsilon: Double, val seed: Option[Long], val sigma: Double, val canopy: Double) extends Serializable with Logging {
  private val seedNum: Long = seed match {
    case Some(sd) => sd
    case None => Utils.random.nextLong()
  }

  require(sigma > 0.0, "sigma值应该大于0")

  private def getWeight(distance: Double): Double = {
    if (distance <= canopy)
      scala.math.exp(-distance * distance / sigma)
    else
      0.0
  }

  /** run函数 */
  def run(data: RDD[Vector]): SOMModel = {
    // 将每个数据正则化。
    val normalizedData = data.map(v => {
      val norm = Vectors.norm(v, 2.0)
      if (norm > 0.0)
        scal(1 / norm, v)
      v
    }) // @todo 判定所有的vector同一长度
    normalizedData.persist()

    val model = runAlgorithm(normalizedData)
    normalizedData.unpersist()
    model
  }


  /** 算法的主运行函数 */
  private def runAlgorithm(value: RDD[Vector]): SOMModel = {
    // step1 初始化model
    var model = initRandom(value, somGrid)

    // step2 迭代求解
    var iter = 0
    var changed = true
    var logArray = new Array[(Double, SOMModel)](10)
    while (iter < maxIterations && changed) {
      val (count, sum, costs) = value.mapPartitions {
        points =>
          val sum = Array.fill(somGrid.getGrid)(Vectors.zeros(numFeatures))
          val count = Array.fill(somGrid.getGrid)(0L)
          var costs = 0.0
          points.foreach { point =>
            val (cost, closestID) = model.centers.map(v => dot(point, v)).zipWithIndex.maxBy(_._1)
            costs += cost

            /** 更新每个closestID对应的全部邻居结点的中心 */
            for (i <- sum.indices) {
              val v = sum(i)
              val weight = if (closestID >= i)
                try {
                  getWeight(somGrid.distance(closestID)(i))
                } catch {
                  case _: Exception => throw new Exception(s"您输入的${closestID}、${i}获取元素失败")
                }
              else
                getWeight(somGrid.distance(i)(closestID))
              if (weight > 0.0)
                axpy(weight, point, v)
            }

            count(closestID) += 1L
          }

          Iterator((count, sum, costs))
      }.reduce {
        case ((arrCount1, arrSum1, costs1), (arrCount2, arrSum2, costs2)) =>
          val arrCount = arrCount1.zip(arrCount2).map {
            case (count1, count2) => count1 + count2
          }
          val arrSum = arrSum1.zip(arrSum2).map { case (v1, v2) =>
            axpy(1.0, v1, v2)
            v2
          }
          (arrCount, arrSum, costs1 + costs2)
      }

      logArray = logArray.drop(1) :+ (costs, model)

      val newCenters = sum.zip(count).map {
        case (v, c) =>
          scal(1.0 / c, v)
          val norm = Vectors.norm(v, 2.0)
          scal(1.0 / norm, v)
          v
      }

      changed = false
      newCenters.zip(model.centers).foreach {
        case (v1, v2) =>
          val dis = dot(v1, v2)
          if (dis > epsilon)
            changed = true
      }

      model = model.updateCenters(newCenters)
      iter += 1
    }

    // step3 输出模型
    logInfo(s"最近10次的误差为${logArray.map(_._1).mkString(",")}")
    logArray.minBy(_._1)._2
  }


  /** 此处建议尽量分散在数据分布的邻域中, 可以利用特征向量 */
  private def initRandom(data: RDD[Vector], somGrid: SOMGrid, format: String = "random"): SOMModel = {
    val gridsNum = somGrid match {
      case som1Grid: SOMGrid1Dims => som1Grid.grids
      case som2Grid: SOMGrid2Dims => som2Grid.xGrids * som2Grid.yGrids
    }

    /**
      * 尽量保证初始向量在数据的分布方向上散布着
      * ----
      * 随机抽取[[gridsNum]]个，向量是归一化的，因此直接在每个向量的元素上做的(-0.05, 0.05)的随机调整
      */
    require(data.count() > 0, s"您的数据为空.")
    val rd = new java.util.Random()

    val centers = try {
      data.takeSample(true, gridsNum, new XORShiftRandom(seedNum).nextInt())
        .toSeq.toArray.map {
        vec =>
          axpy(1.0, new DenseVector(Array.fill(numFeatures)(rd.nextDouble() * 0.1 - 0.05)), vec)
          val norm = Vectors.norm(vec, 2.0)
          scal(if (norm == 0.0) 1.0 else 1 / norm, vec)
          vec
      }
    } catch {
      case e: Exception => throw new Exception(s"在获得初始权重是出现错误，具体信息：${e.getMessage}")
    }

    new SOMModel(somGrid, centers, numFeatures)
  }


}


/**
  * SOM的输出模型
  * ----
  *
  * @param somGrid 拓扑结构
  * @param centers 每个神经元对应的向量
  *
  */
class SOMModel(val somGrid: SOMGrid, val centers: Array[Vector], val numFeatures: Int) extends Serializable {
  def this(somGrid: SOMGrid, centers: Array[Vector]) = this(somGrid, centers, 1)

  private[mllib] def updateCenters(newCenters: Array[Vector]): SOMModel = {
    require(centers.length == newCenters.length, "SOM模型两次更换是中心应该一致")
    new SOMModel(somGrid, newCenters, numFeatures)
  }

  def predict(value: Vector): Int = {
    centers.map { vec => dot(vec, value) }.zipWithIndex.maxBy(_._1)._2
  }

  def predictAxisVector(value: Vector): Vector = {
    val id = predict(value)
    somGrid match {
      case _: SOMGrid1Dims =>
        Vectors.dense(Array(id.toDouble))
      case som2Grid: SOMGrid2Dims =>
        Vectors.dense(Array(id / som2Grid.yGrids, id % som2Grid.yGrids).map(_.toDouble))
    }
  }

  def save(sQLContext: SQLContext, path: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    require(!fs.exists(new Path(path, "data")), s"您输入的路径${path}已经存在, 算子为了安全机制不会覆盖写入。")

    sQLContext.createDataFrame(Seq((somGrid.saveAsString, centers, numFeatures)))
      .toDF(SOMModel.SaveLoadV1_0.somGridInfoName,
        SOMModel.SaveLoadV1_0.centersInfoName,
        SOMModel.SaveLoadV1_0.numFeaturesName)
      .write
      .mode("error")
      .parquet(Loader.dataPath(path))
  }

}


object SOMModel {
  def load(sQLContext: SQLContext, path: String): SOMModel =
    SaveLoadV1_0.load(sQLContext: SQLContext, path: String)

  /** 读写的工具类 */
  object SaveLoadV1_0 {
    def load(sQLContext: SQLContext, path: String): SOMModel = {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      require(fs.exists(new Path(path, "data")), s"您输入的路径${path}不存在")
      val infoDF = sQLContext.read.parquet(Loader.dataPath(path))
        .select(somGridInfoName, centersInfoName, numFeaturesName)
      try {
        infoDF.head.toSeq match {
          case Seq(somGridInfo: String, centersInfo: Seq[Vector], numFeatures: Int) =>
            var map = Map[String, Int]()
            somGridInfo.split(",").foreach {
              s =>
                val split = s.trim.split(":")
                map += (split(0) -> split(1).toDouble.toInt)
            }
            val sOMGrid = map("dims") match {
              case 1 =>
                if (map("topology") == 0)
                  new SOMGrid1Dims(map("grids"), "linear")
                else
                  throw new Exception("目前一维拓扑支持线性的模型")
              case 2 =>
                if (map("topology") == 0)
                  new SOMGrid2Dims(map("xGrids"), map("yGrids"), "rectangular")
                else
                  new SOMGrid2Dims(map("xGrids"), map("yGrids"), "hexagonal")
              case _ =>
                throw new Exception("目前只支持一维和二维的拓扑结构")
            }

            new SOMModel(sOMGrid, centersInfo.toArray, numFeatures)

          case _ =>
            throw new Exception("在保存的记录中不是想要的类型，读取模型失败")
        }

      } catch {
        case e: Exception => throw new Exception(s"在获取模型信息中失败，具体异常为${e.getMessage}")
      }

    }

    val somGridInfoName = "somGrid"
    val centersInfoName = "centers"
    val numFeaturesName = "numFeatures"

  }


}

private[mllib] trait SOMGrid extends Serializable {
  def getGrid: Int

  val distance: muMap[Int, muMap[Int, Double]] = {
    val m = muMap.empty[Int, muMap[Int, Double]]
    for (i <- 0 until getGrid) {
      for (j <- 0 to i) {
        val iElement = m.getOrElse(i, muMap.empty[Int, Double])
        if (iElement.get(j).isEmpty)
          iElement += (j -> findDistance(i, j))
        m += (i -> iElement) // 根据对称性，如果j <= i时 m(i)(j)，否则m(j)(i)
      }
    }
    m
  }

  def saveAsString: String

  def findDistance(one: Int, another: Int): Double
}


object SOMUtils {

  def recGrids2HexGrids(x: Double, y: Double, length: Int = 1): (Double, Double) =
    (x * length + (y + 1) % 2 * 0.5 * length, y * scala.math.sqrt(3) / 2 * length)

}


/**
  * 二维的SOM竞争层网络结构
  * ----
  *
  * @param xGrids   横轴结点个数
  * @param yGrids   纵轴结点个数
  * @param topology 拓扑结构 --只能是"rectangular", "hexagonal"之一
  */
class SOMGrid2Dims(val xGrids: Int, val yGrids: Int, val topology: String) extends SOMGrid {
  require(topology == "rectangular" || topology == "hexagonal",
    "二维时topology需要为rectangular或hexagonal之一")

  override def getGrid: Int = xGrids * yGrids

  /**
    * 获得id在坐标轴中的坐标
    * ----
    * 排列方式为从下到上从左到右
    * 2, 5, ...
    * 1, 4, ...
    * 0, 3, ...
    *
    * @param id id
    * @return
    */
  def getCoordinate(id: Int): (Int, Int) = (id / yGrids, id % yGrids)

  def this(xGrids: Int, yGrids: Int) = this(xGrids: Int, yGrids: Int, "hexagonal")

  override def findDistance(one: Int, another: Int): Double = {
    require(one < getGrid && another < getGrid)
    val (oneX, oneY) = (one / xGrids, one % yGrids) // 从左下方计入坐标
    val (anotherX, anotherY) = (another / xGrids, another % yGrids)

    topology match {
      case "rectangular" =>
        scala.math.sqrt((oneX - anotherX) * (oneX - anotherX) + (oneY - anotherY) * (oneY - anotherY)) // 边长为1

      case "hexagonal" =>
        val (x1, y1) = SOMUtils.recGrids2HexGrids(oneX, oneY)
        val (x2, y2) = SOMUtils.recGrids2HexGrids(anotherX, anotherY)
        scala.math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2))
    }

  }

  override def saveAsString: String =
    Seq(s"dims:${2}", s"topology:${if (topology == "rectangular") 0 else 1}",
      s"xGrids:$xGrids", s"yGrids:$yGrids").mkString(",")

}


/**
  * 一维的SOM竞争层网络结构
  * ----
  *
  * @param grids    结点个数
  * @param topology 拓扑结构 --目前只能是"linear"
  */
class SOMGrid1Dims(val grids: Int, val topology: String) extends SOMGrid {
  require(topology == "linear", "一维时topology只支持linear")

  override def getGrid: Int = grids

  def this(grids: Int) = this(grids, "linear")

  def getCoordinate(id: Int): Int = id

  override def findDistance(one: Int, another: Int): Double =
    if (one >= another)
      one - another
    else
      another - one

  override def saveAsString: String =
    Seq(s"dims:${1}", s"topology:${if (topology == "linear") 0 else 1}", s"grids:$grids").mkString(",")
}