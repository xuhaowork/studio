package com.self.core.polr.models

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

object Utils extends Serializable {
  def getRddIdByDouble(rawDataDF: DataFrame,
                       idColName: String,
                       idColType: String,
                       featureCols: ArrayBuffer[(String, String)]): RDD[(Double, DenseVector)] =
    rawDataDF.rdd.map(r => {
      val id: Double = idColType match {
        case "int" => util.Try(r.getAs[Int](idColName).toDouble) getOrElse Double.NaN
        case "float" => util.Try(r.getAs[Float](idColName).toDouble) getOrElse Double.NaN
        case "double" => util.Try(r.getAs[Double](idColName)) getOrElse Double.NaN
        case "string" => util.Try(r.getAs[String](idColName).trim.toDouble) getOrElse Double.NaN
        case "long" => util.Try(r.getAs[Long](idColName).toDouble) getOrElse Double.NaN
        case _ => throw new Exception("暂不支持其他类型的因变量列")
      }

      val features = featureCols.map {
        case (name, dataType) => dataType match {
          case "int" => util.Try(r.getAs[Int](name).toDouble) getOrElse 0.0
          case "float" => util.Try(r.getAs[Float](name).toDouble) getOrElse 0.0
          case "double" => util.Try(r.getAs[Double](name)) getOrElse 0.0
          case "string" => util.Try(r.getAs[String](name).toDouble) getOrElse 0.0
          case "long" => util.Try(r.getAs[Long](name).toDouble) getOrElse 0.0
          case _ => throw new Exception("暂不支持其他类型的因变量列")
        }
      }.toArray
      (id, new DenseVector(features))
    })


  def getRddIdByString(rawDataDF: DataFrame,
                       idColName: String,
                       idColType: String,
                       featureCols: ArrayBuffer[(String, String)]): RDD[(String, DenseVector)] =
    rawDataDF.rdd.map(r => {
      val id: String = idColType match {
        case "int" => util.Try(r.getAs[Int](idColName).toString) getOrElse null
        case "float" => util.Try(r.getAs[Float](idColName).toString) getOrElse null
        case "double" => util.Try(r.getAs[Double](idColName).toString) getOrElse null
        case "string" => util.Try(r.getAs[String](idColName).trim) getOrElse null
        case "long" => util.Try(r.getAs[Long](idColName).toString) getOrElse null
        case _ => throw new Exception("暂不支持其他类型的因变量列")
      }

      val features = featureCols.map {
        case (name, dataType) => dataType match {
          case "int" => util.Try(r.getAs[Int](name).toDouble) getOrElse 0.0
          case "float" => util.Try(r.getAs[Float](name).toDouble) getOrElse 0.0
          case "double" => util.Try(r.getAs[Double](name)) getOrElse 0.0
          case "string" => util.Try(r.getAs[String](name).toDouble) getOrElse 0.0
          case "long" => util.Try(r.getAs[Long](name).toDouble) getOrElse 0.0
          case _ => throw new Exception("暂不支持其他类型的因变量列")
        }
      }.toArray
      (id, new DenseVector(features))
    })



  def transform(sc: SparkContext, rdd: RDD[(String, DenseVector)],
                categories: Map[String, Double], featureColsSize: Int)
  : (RDD[LabeledPoint], Map[String, Double]) = {
    val categoriesNum = categories.size
    if (rdd.count() <= 1 + categoriesNum + featureColsSize)
      throw new Exception("不满足过度识别")
    if (categoriesNum >= 10000)
      throw new Exception("分类数大于10000, collect过多严重影响性能")

    val categoriesBC = sc.broadcast(categories).value
    (rdd.mapPartitions {
      iter =>
        iter.map {
          case (key, features) => LabeledPoint(categoriesBC.getOrElse(key, Double.NaN), features)
        }.filter(!_.label.isNaN)
    }, categoriesBC)
  }


  def transform(sc: SparkContext, rdd: RDD[(Double, DenseVector)], featureColsSize: Int)
  : (RDD[LabeledPoint], Map[Double, Double]) = {
    val keys = rdd.keys.distinct()
    val categoriesNum = keys.count()
    if (rdd.count() <= 1 + categoriesNum + featureColsSize)
      throw new Exception("不满足过度识别")
    if (categoriesNum >= 10000)
      throw new Exception("分类数大于10000, 会导致collect过多严重影响性能。导致该问题产生的原因可能是：1）您没有将Double类型的数据做分箱处理。2）数据中确实有如此多的级别，如果您确实有该需求请联系开发人员。")
    val categories = keys.collect().sorted.zipWithIndex.map {
      case (key, index) => (key, index.toDouble)
    }.toMap

    val categoriesBC = sc.broadcast(categories).value
    (rdd.mapPartitions {
      iter =>
        iter.map {
          case (key, features) => LabeledPoint(categoriesBC.getOrElse(key, Double.NaN), features)
        }.filter(!_.label.isNaN)
    }, categoriesBC)

  }


}
