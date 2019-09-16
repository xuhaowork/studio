package cn.datashoe.dataWrangling

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, udfWithNull}

import scala.collection.mutable.ArrayBuffer
import scala.math.abs

/**
  * 基于均值迭代的离散化方法
  */
object DiscretizationByMeansIter {
  /**
    * 算法
    * ----
    * 1)随机选取一条数据初始化center并记录到centers
    * 2)找到位于标准值 +- 误差范围外的数据
    * 3)当2中的数据不为空时
    * - 求均值
    * - 新center = if(均值值 == center) 在2的数据中随机选择一条作为center else 均值 // 防止掉入均值陷阱
    * - 记录center到centers
    * 当2中的数据为空时终止
    * 4)重复2/3直至收敛
    * 5)根据centers找每条数据最接近的center作为预测值.
    */
  /**
    * 训练获得centers
    *
    * @param data    数据
    * @param colName 列名
    * @param delta   误差允许范围
    * @param seed    随机数
    * @return 中心
    */
  def train(data: DataFrame, colName: String, delta: Double, seed: Long): Array[Double] = {
    var rdd = data.select(colName).rdd.map {
      row =>
        val value = util.Try(row.get(row.fieldIndex(colName)).toString.toDouble).getOrElse(Double.NaN)
        (value, 1L)
    }.filter { case (value, _) => !value.isNaN }

    rdd.cache()
    import scala.math.abs

    var center = try {
      rdd.takeSample(false, 1, seed).head._1
    } catch {
      case e: Exception =>
        throw new Exception(s"在获取数据的初始中心时失败, 请检查数据是否为空或者是否有正确的数值. " +
          s"该异常的具体信息为: ${e.getMessage}")
    }
    val centers = ArrayBuffer(center)
    var count = 1L

    var i = 0
    while (count > 0L && i <= 10) {
      rdd = rdd.map {
        case (value, flag) =>
          if (flag == 1L && abs(center - value) > delta) {
            (value, 1L)
          } else {
            (0.0, 0L)
          }
      }

      rdd.cache()

      //          | 6154|  6154 8253
      //          | 8360|
      //          | 5641|
      //          | 4934|
      //          |11943|
      //          |11171|
      //          | 2063|
      //          |10977|
      //          | 2610|
      //          | 9111|
      //          |11830|
      //          | 9749|
      //          |12835|
      //          |12387|
      //          | 4339|
      //          | 5375|
      //          | 3665|
      //          |10097|
      //          | 3514|
      //          |10728|

      val res = rdd.reduce {
        case ((value1, count1), (value2, count2)) =>
          (value1 + value2, count1 + count2)
      }

      //        println("-" * 80)
      //        println("center: " + center)
      //        println("count:" + count)
      //        rdd.zipWithIndex().sortBy(_._2).foreach(println)

      val newCenter = res._1 / res._2
      if (res._2 > 0L) {
        if (newCenter != center) {
          center = newCenter
        } else {
          center = rdd.filter { case (_, cnt) => cnt > 0 }.takeSample(false, 1, seed).head._1
        }
        centers += center
      }
      count = res._2
      i += 1
    }

    //    println(centers.mkString(",  "))
    centers.toArray
  }


  /**
    * 预测
    *
    * @param data      数据
    * @param colName   列名
    * @param centers   离散化的中心
    * @param outputCol 输出列名
    * @return
    */
  def predict(data: DataFrame, colName: String, centers: Array[Double], outputCol: String): DataFrame = {
    val transform = udfWithNull.udf {
      value: Double =>
        centers.minBy(center => abs(value - center))
    }
    data.withColumn(outputCol, transform(col(colName).cast(DoubleType)))
  }


}
