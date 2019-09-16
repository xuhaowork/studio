package com.self.core.ART1NeuralNet.model

import breeze.linalg.{DenseMatrix, DenseVector}

/**
  * ART1模型
  * ----
  *
  * @param rho         理解的敏锐度，敏锐度越高越能够识别模式的差异，同时知识负担也更重
  * @param numFeatures 向量特征数
  * @param wMatrix     由输入层到识别层的连接权
  * @param maxNeurons  最大的神经元数，当超出该神经元时不再学习
  * @param knowledge   知识 --用以学习新事物，当新事物能够以现有知识理解时就用现有知识替代，否则新建一个模式，会不断更新
  */
class ARTModel(
                val rho: Double,
                val numFeatures: Int,
                var wMatrix: DenseMatrix[Double],
                var maxNeurons: Int,
                val knowledge: scala.collection.mutable.Map[Int, DenseVector[Double]]
              ) extends Serializable {

  /**
    * 从数据中学习
    *
    * @param data 一条数据  每学习一条数据模型就会变化一些
    * @return 模型自身会成长
    */
  def learn(data: DenseVector[Double]): this.type =
    if (knowledge.size > maxNeurons) {
      this
    } else {
      require(data.length == numFeatures, s"您输入的数据长度需要和首行保持一致，首行数据长度为$numFeatures")
      /** 1)有内星权向量激活进入识别层 */
      val recVec: DenseVector[Double] = wMatrix * data // 模式匹配 向量呈上内星权进入识别层

      /** 2)识别层找到最合适的知识，如果找不到新建一个知识 */
      val valueWithIndex: Array[(Double, Int)] = recVec.data.zipWithIndex
      val sum = data.data.sum

      var neurons = valueWithIndex
      var flag = false // 是否找到匹配的知识

      var maxIndex = -1
      var maxTij = DenseVector.zeros[Double](1)
      var similarity = Double.NaN // 相似度

      while (!neurons.isEmpty && !flag) {
        maxIndex = neurons.maxBy(_._1)._2
        maxTij = knowledge(maxIndex)

        similarity = maxTij dot data

        if (similarity / sum >= rho) { // 如果在敏感度范围内认为这是相似的，则标识找到合适的知识来匹配该数据
          flag = true
        } else {
          val drop = neurons.map(_._1).zipWithIndex.maxBy(_._1)._2
          neurons = neurons.slice(0, drop) ++ neurons.slice(drop + 1, neurons.length)
        }
      }


      if (flag) { // 标识找到合适的, 并更新合适的权值
        val vl = maxTij :* data
        knowledge += (maxIndex -> vl)
        wMatrix(maxIndex, ::).inner := vl :* (1 / (1 - 1.0 / numFeatures + similarity))
      } else { // 没找到合适的, 新建一个知识, 并增加权值维度
        maxIndex = wMatrix.rows
        wMatrix = DenseMatrix.vertcat(wMatrix, DenseMatrix.ones[Double](1, numFeatures) :* (1.0 / (1 + numFeatures)))
        knowledge += (maxIndex -> data)
      }

      this
    }

  /**
    * 预测
    * 模型以现有的情况下对数据的预测，只预测。如果在理解范围内则给出适合的模型，如果没有新建一个模型。
    * 但它只是预测，模型并不会成长
    *
    * @param data 数据
    * @return 预测的类型id和最匹配的知识
    */
  def predict(data: DenseVector[Double]): (Int, DenseVector[Double]) = {
    require(data.length == numFeatures, s"您输入的数据长度需要和首行保持一致，首行数据长度为$numFeatures")
    /** 1)有内星权向量激活进入识别层 */
    val recVec: DenseVector[Double] = wMatrix * data // 模式匹配 向量呈上内星权进入识别层

    /** 2)识别层找到最合适的知识，如果找不到新建一个知识 */
    val valueWithIndex: Array[(Double, Int)] = recVec.data.zipWithIndex
    val sum = data.data.sum

    var neurons = valueWithIndex
    var flag = false // 是否找到匹配的知识

    var maxIndex = -1
    var maxTij = DenseVector.zeros[Double](1)
    var similarity = Double.NaN // 相似度

    while (!neurons.isEmpty && !flag) {
      maxIndex = neurons.maxBy(_._1)._2
      maxTij = knowledge(maxIndex)

      similarity = maxTij dot data

      if (similarity / sum >= rho) { // 如果在敏感度范围内认为这是相似的，则标识找到合适的知识来匹配该数据
        flag = true
      } else {
        val drop = neurons.map(_._1).zipWithIndex.maxBy(_._1)._2
        neurons = neurons.slice(0, drop) ++ neurons.slice(drop + 1, neurons.length)
      }
    }


    if (flag) { // 找到合适的, 更新合适的权值
      (maxIndex, try {
        knowledge(maxIndex)
      } catch {
        case _: Exception => throw new Exception(s"在知识的模式中没有找到第${maxIndex}个类型")
      })
    } else { // 没找到合适的, 新建一个知识
      (wMatrix.rows, data)
    }
  }

}


object ARTModel {
  def init(rho: Double = 0.2, numFeatures: Int, maxNeurons: Int = 200): ARTModel = {
    val initW = DenseMatrix.ones[Double](1, numFeatures) :* (1.0 / (1 + numFeatures))
    new ARTModel(rho, numFeatures, initW, maxNeurons,
      scala.collection.mutable.Map(0 -> DenseVector.ones[Double](numFeatures))
    )
  }

}
