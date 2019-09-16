package com.self.core.SOM

import com.self.core.baseApp.myAPP
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.clustering.{SOMGrid1Dims, SOMGrid2Dims, SOMModel}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.sql.SQLContext

/**
  * editor：Xuhao
  * date： 2018/7/17 10:00:00
  */

/**
  * SOM算法
  * ----
  * 自组织映射神经网络 --Self Organizing Map
  * 是一种无监督学习算法，最终的输出结果是带有拓扑结构的聚类，因此划归入聚类算法。
  * 参考文献： @see The element of statistical learning 14.4, 这里实现的是批处理模式
  * ----
  * 虽然叫做自组织神经网络但，由于其本质上是一种无监督学习的聚类算法，没有误差的反向传播等机制，
  * 算法本质上的迭代过程更接近于KMeans
  * ----
  * 改进脚本实现的是批处理版本：
  * 1）相对于online版本没有学习率递减和邻域递减
  * 2）终止条件是神经元(‘聚类’)中心不再发生变化
  * 3）邻域函数目前是高斯函数
  * 4）神经元之间的拓扑结构可以是一维或二维，二维结构可以是矩形或正六边形。
  * --拓扑结构的作用：拓扑结构决定了神经元之间的距离，从而决定邻域，决定了神经元的更新。
  */

object SaveLoadTest extends myAPP {
  override def run(): Unit = {

    val rd = new java.util.Random(123L)

    val path = "/data/somModels/"
    val pth = new Path(path, "data").toUri.toString

    println(pth)

    val sOMGrid = Seq(s"dims:${2}", s"topology:0", "xGrids:5", s"yGrids:5").mkString(",")
    val centers = Array.fill(25)(new DenseVector(Array.fill(3)(rd.nextGaussian())))
    val df = sqlc.createDataFrame(Seq((sOMGrid, centers)))
      .toDF("somGrid", "centers")

    df.write.mode("overwrite").parquet(pth)


    val rddTableName = "<#zzjzRddName#>"
    outputrdd.put(rddTableName, path)
    outputrdd.put(rddTableName + "_模型路径", df)


    object SaveLoadV1_0 {
      def load(sQLContext: SQLContext, path: String): SOMModel = {
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        require(fs.exists(new Path(path, "data")), s"您输入的路径${path}不存在")

        val pth = new Path(path, "data").toUri.toString
        val infoDF = sQLContext.read.parquet(pth).select("somGrid", "centers")

        try {
          infoDF.head.toSeq match {
            case Seq(somGridInfo: String, centersInfo: Seq[Vector]) =>
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

              new SOMModel(sOMGrid, centersInfo.toArray)
          }
        } catch {
          case e: Exception => throw new Exception(s"在获取模型信息中失败，具体异常为${e.getMessage}")
        }

      }

      val somGridInfoName = "somGrid"
      val centersInfoName = "centers"
    }


    val path2 = ""
    val mdl = SaveLoadV1_0.load(sqlc, path2)
    mdl.centers.foreach(println)
    println(mdl.predict(new DenseVector(Array(0.1, 0.9, 5.0))))


  }
}
