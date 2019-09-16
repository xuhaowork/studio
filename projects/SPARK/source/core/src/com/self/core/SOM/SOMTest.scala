package com.self.core.SOM

import com.self.core.baseApp.myAPP
import org.apache.spark.SparkException
import org.apache.spark.mllib.clustering.{SOM, SOMGrid2Dims, SOMModel}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}


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

object SOMTest extends myAPP {
  override def run(): Unit = {

    /** 模拟数据 */
    val rd = new java.util.Random(123L)

    // 三个中心
    val clusters = Array(Array(1.1, 2.0, 3.5), Array(-1.5, 2.0, -3.5), Array(0.0, 0.0, 0.0))

    import org.apache.spark.mllib.linalg.Vectors
    val data = clusters.zipWithIndex.flatMap {
      case (arr, idx) => Array.tabulate(1000)(_ => (Vectors.dense(arr.map(_ + rd.nextGaussian())), idx))
    }

    val data2 = Seq(
      ("鸽子", "1001000010010"),
      ("鸡", "1001000010000"),
      ("鸭", "1001000010001"),
      ("野鸭", "1001000010011"),
      ("猫头", "1001000011010"),
      ("隼", "1001000011010"),
      ("金雕", "0101000011010"),
      ("狐狸", "0100110001000"),
      ("狗", "0100110000100"),
      ("狼", "0100110001100"),
      ("猫", "1000110001000"),
      ("虎", "0010110001100"),
      ("狮", "0010110001100"),
      ("马", "0010111100100"),
      ("斑马", "0010111100100"),
      ("牛", "0010111000000")
    ).map {
      case (animal, features) =>
        (animal, Vectors.dense(features.map(_.toDouble).toArray))
    }

//    val rawDataFrame = sqlc.createDataFrame(data2).toDF("vec", "cluster")

    val rawDataFrame = sqlc.createDataFrame(data2).toDF("animal", "features")

    rawDataFrame.show()


    val somGrid = new SOMGrid2Dims(4, 4, "rectangular")

    val numFeatures = "0010111000000".length
    val som = new SOM(somGrid, numFeatures, 2000, 1E-4, Some(123L), 2.5, 3.0)

    val inputData = rawDataFrame.select("animal", "features").rdd.map { row =>
      val vec = row.get(1) match {
        case dv: DenseVector => dv
        case sv: SparseVector => sv
        case null => throw new SparkException("不支持null类型的输入，请过滤掉null值重试")
        case _ => throw new SparkException("您输入的不是向量类型数据")
      }
      val cluster = row.get(0) match {
//        case i: Int => i.toDouble
//        case d: Double => d
        case s: String => s
      }

      (cluster, vec)
    }


    val model: SOMModel = som.run(inputData.values)

    inputData.map {
      case (_, vec) =>
        model.predictAxisVector(vec)
    }.foreach(println)


    val result = inputData.map {
      case (cluster, vec) =>
        val coordinate = model.somGrid match {
          case sg2: SOMGrid2Dims => sg2.getCoordinate(model.predict(vec))
        }
        (cluster, vec, coordinate)
    }


    val resultMap = result.map {
      case (cluster, _, coordinate) => (coordinate, Array(cluster.toInt))
    }.reduceByKey((arr1, arr2) => arr1 ++ arr2).collectAsMap()

    for (x <- (0 until 5).reverse) {
      print(x + ":|  ")
      for (y <- 0 until 5) {
        val f = if (resultMap.contains((x, y))) {
          resultMap.apply((x, y)).groupBy(i => i).map {
            case (key, values) => (key, values.length)
          }.maxBy(_._2)._1
        } else {
          0
        }

        //        val s = resultMap.apply((x, y)).distinct.map(_.toString)
        //        val s1 = s ++ new Array[String](3 - s.length).map(_ => " ")

        print(f + "   ")
      }
      println()

    }

    println("--|-------------------")
    print(" :|  ")
    for (i <- 0 until 5) {
      print(i + "   ")
    }
    println()


  }
}
