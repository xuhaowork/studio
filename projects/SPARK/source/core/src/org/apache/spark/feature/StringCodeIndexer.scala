package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.util.collection.OpenHashMap

class StringCodeIndexer extends Serializable {
  var inputCol = ""
  var outputCol = ""

  def setInputCol(inputCol: String): this.type = {
    this.inputCol = inputCol
    this
  }

  def setOutputCol(value: String): this.type = {
    this.outputCol = outputCol
    this
  }

  def fit(dataset: DataFrame): StringCodeIndexerModel = {
    val labels: Array[String] = try {
      dataset.select(col(inputCol))
        .rdd
        .map(_.getString(0))
        .distinct()
        .collect()
    } catch {
      case e: Exception =>
        throw new Exception(s"在通过driver端获取所有分区中不同的'$inputCol'时失败，具体信息:${e.getMessage}")
    }

    new StringCodeIndexerModel(labels)
  }

}

class StringCodeIndexerModel(labels: Array[String]) extends Serializable {
  val labelToIndex: OpenHashMap[String, Int] = {
    val n = labels.length
    val map = new OpenHashMap[String, Int](n)
    var i = 1 // 从1开始，因为0索引目前会碰到问题
    while (i <= n) {
      map.update(labels(i - 1), i)
      i += 1
    }
    map
  }

  val indexToLabel: OpenHashMap[Int, String] = {
    val n = labels.length
    val map = new OpenHashMap[Int, String](n)
    var i = 1 // 从1开始，因为0索引目前会碰到问题
    while (i <= n) {
      map.update(i, labels(i - 1))
      i += 1
    }
    map
  }

  def encode2Int(string: String): Int = try{
    labelToIndex(string)
  }catch {
    case e: Exception => throw new SparkException(s"没有找到'$string'对应索引，具体信息：${e.getMessage}")
  }

  def decode2String(index: Int): String = try{
    indexToLabel(index)
  }catch {
    case e: Exception => throw new SparkException(s"没有找到'$index'对应索引，具体信息：${e.getMessage}")
  }


}
