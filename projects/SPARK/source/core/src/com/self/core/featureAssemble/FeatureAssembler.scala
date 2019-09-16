package com.self.core.featureAssemble

import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.mllib.util.VectorBLAS.axpy
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * editor: xuhao
  * date: 2018-09-26 08:30:00
  */

/**
  * 特征提取模块
  *
  * @see input 在该类别之前将多个表做成长的事件表并去掉无效值，作为[[FeatureAssembler]]的输入，注意这里不在去除无效值
  *      ----
  *      形式为: [keyString, feature, frequency] 其中feature是对应特征的值, frequency是对应特征的频数，
  *      --这里的特征是结合特征列名进行编码后的特征
  *      --for: 这样做的目的是为了防止如源IP和对端IP两个特征但值是完全一样的情况。
  * @define 输入参数
  *               ----
  *               1)keyString/feature/category的列名
  *               2)
  * @define train 训练事件表，获得特征集成的模型[[FeatureAssemblerModel]]
  * @param maxFeatureNum 最大特征数，防止栈溢出
  * @param tfIdf         是否进行tf-idf转换
  */
class FeatureAssembler(
                        val maxFeatureNum: Long,
                        val tfIdf: Boolean,
                        val min_docFreq: Double = 5.0
                      ) {
  /** 默认参数和一些参数设定函数 */
  def this() = this(10000L, true, 5.0)

  /**
    * 长表的一些默认列名
    * ----
    * 为了降低参数的复杂度将其固定
    */
  private var (keyStringCol, featureCol, frequencyCol) = ("keyString", "feature", "category")

  /**
    * set和get方法
    */
  def setKeyStringCol(keyStringCol: String): this.type = {
    this.keyStringCol = keyStringCol
    this
  }

  def setFeatureCol(featureCol: String): this.type = {
    this.featureCol = featureCol
    this
  }

  def setCategoryCol(categoryCol: String): this.type = {
    this.frequencyCol = categoryCol
    this
  }

  /**
    * 通过训练获得稀有特征集
    *
    * @param frequencyRDD 输入数据
    *                     特征依次是：keyString/feature/frequency 类型必须是[String, String, Long]
    * @return
    */
  def train(frequencyRDD: RDD[((String, String), Long)]): FeatureAssemblerModel = try {
    /** 生成语料库及对应的文档频率 --docFreqMap: [语料库中的词汇, _, 文档频率] */
    val docFreqMap: Map[String, (Double, Int)] = frequencyRDD
      .map {
        // 以每个keyString为一个文档，同时统计: 特征 ~ 特征频率, 特征 ~ 文档频率
        case ((_, featureWithLabel), freq) => (featureWithLabel, (freq, 1.0))
      }.reduceByKey {
      case ((freq1, docFreq1), (freq2, docFreq2)) =>
        (freq1 + freq2, docFreq1 + docFreq2)
    }.filter {
      case (_, (_, docFreq)) =>
        docFreq >= min_docFreq // 低于最小文档频率的作为噪声过滤掉
    }.top(maxFeatureNum.toInt)(Ordering.by[(String, (Long, Double)), Long](_._2._1)) // 根据总频率取top n作为语料库
      .zipWithIndex.map { // 为语料库加上索引，保证语料库顺序一致，方便预测时的向量构建
      case ((key, (_, docFreq)), index) => // 此时总频率没有意义了，丢弃之
        (key, (docFreq, index))
    }.toMap // 这里count是freq出现在每个keyString的次数

    /**
      * 逆文档频率变换 --IDF
      * ----
      * 功能：
      * 考虑词汇在文档中的稀有性，某个词汇出现在文档中越稀有(要高于最小文档数[[min_docFreq]]才有效，否则作为噪声词汇)则IDF越大，
      * 和文档中的词频TF分别侧重两个方面
      * ----
      * 每个词汇[ word ]在文档[ doc_i ]中的逆文档频率计算公式为：
      * idf(doc_i, word) = log { (all_doc_freq + 1) / (doc_freq(doc_i, word) + 1) }
      */
    val all_doc_freq = frequencyRDD.map {
      case ((keyString, _), _) => (keyString, 1.0)
    }.reduceByKey(_ + _).count().toDouble // 文档数：即keyString个数
    val sparseFeatures = docFreqMap.mapValues {
      case (freq, index) =>
        (scala.math.log((all_doc_freq + 1.0) / (freq + 1.0)), index)
    }

    new FeatureAssemblerModel(sparseFeatures)
  } catch {
    case e: Exception => throw new Exception(s"在获得通过训练获得稀有特征集时，具体信息为: ${e.getMessage}")
  }

}

/**
  * [[com.self.core.featureAssemble.FeatureAssembler]]的train算法 --目的是获得最常用的[[FeatureAssembler.maxFeatureNum]]个特征：
  * 1)将所有的feature和category结合编码，认为是一个keyString所经过的一个值（或者认为是轨迹点）
  *
  * 2)取最长出现的[[com.self.core.featureAssemble.FeatureAssembler.maxFeatureNum]]个特征，作为稀疏特征，存入模型
  *
  * @define load    模型加载
  * @define save    模型保存
  * @define predict 预测模型
  */
class FeatureAssemblerModel(
                             val sparseFeatures: scala.collection.Map[String, (Double, Int)],
                             val alpha: Double = 0.0
                           ) extends Serializable {
  val featuresNum: Int = sparseFeatures.size


  /**
    * 保存模型到HDFS
    *
    * @param sQLContext SQLContext
    * @param path       保存路径
    * @param saveMode   允许两种形式
    *                   overwrite该形式可能会覆盖其他文件，业务场景下可以用，慎用；
    *                   append 外层禁用，这里没有意义
    *                   ignore 外层禁用
    *                   default 如果存在则抛出异常，默认模式
    */
  def save(sQLContext: SQLContext, path: String, saveMode: String = "default"): Unit = {
    val validPath = try {
      new Path(path, "data").toUri.toString
    } catch {
      case e: Exception => throw new Exception(s"在稀疏特征集成时物理化模型生成路径'$path'出现问题：${e.getMessage}")
    }
    val df = sQLContext.createDataFrame(
      sparseFeatures.map { case (feature, (docFreq, index)) => (feature, docFreq, index) }.toArray
    ).toDF("feature", "documentFreq", "index")

    df.write.mode(saveMode).parquet(validPath)
  }


  /**
    * 预测
    *
    * @param frequencyRDD 需要预测的数据
    *                     字段含义为: ((keyString, 特征), 频次)
    *                     note：这里的频次和特征频次统计可以有血缘关系，从而训练和预测共用一个输入数据，以节省性能
    * @return 预测后的输出，
    *         字段含义为: keyString, 稀疏特征
    */
  def predict(frequencyRDD: RDD[((String, String), Long)]): RDD[(String, SparseVector)] = {
    val zeroValue: SparseVector = Vectors.zeros(featuresNum).toSparse
    val seqOp = (res: SparseVector, tup: (String, Long)) => {
      tup match {
        case (feature, frequency) => // tf
          val (docFreq, index) = sparseFeatures.getOrElse(feature, (0.0, -1)) // idf
          if (index >= 0) {
            axpy(1.0, new SparseVector(featuresNum, Array(index), Array(docFreq * frequency)), res) // tf * idf
            res
          } else {
            zeroValue
          }
      }
    }
    val combOp = (res1: SparseVector, res2: SparseVector) => {
      axpy(1.0, res2, res1)
      res1
    } // 频率非连续线性变换 --用于权衡数据的频率特性和属性特性
    frequencyRDD.map {
      case ((keyString, feature), frequency) =>
        (keyString, (feature, frequency))
    }.aggregateByKey(zeroValue)(
      seqOp,
      combOp
    ).mapValues(utils.sigRectifyVector(_, alpha))

  }


}


object FeatureAssemblerModel {
  /**
    * 从物理化存储中读取模型
    *
    * @param sQLContext SQLContext
    * @param path       模型保存路径
    * @return 读取的模型
    */
  def load(sQLContext: SQLContext, path: String): FeatureAssemblerModel = {
    val validPath = try {
      new Path(path, "data").toUri.toString
    } catch {
      case e: Exception => throw new Exception(s"在稀疏特征集成时物理化模型生成路径'$path'出现问题：${e.getMessage}")
    }
    val sparseFeatures = sQLContext.read.parquet(validPath).rdd.map {
      row =>
        (row.getAs[String]("feature"), (row.getAs[Double]("documentFreq"), row.getAs[Int]("index")))
    }.collectAsMap()

    new FeatureAssemblerModel(sparseFeatures)
  }

}



