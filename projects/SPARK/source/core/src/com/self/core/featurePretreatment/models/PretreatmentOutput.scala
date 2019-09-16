package com.self.core.featurePretreatment.models

import org.apache.spark.sql.DataFrame

/**
  * 抽象类Pretreater类型的输出接口
  * ----
  * 约定：
  * 1)Pretreater至少输出一个DataFrame
  * 2)还可能输出一个变换的辅助信息——AuxiliaryInfo，后面可能用到。
  *
  * @param data 输出数据
  * @define getData 获得输出数据
  *
  */
abstract class PretreatmentOutput(val data: DataFrame) {
  def getData: DataFrame
}


/**
  * 特征预处理中可能输出的一些其他信息
  */
trait AuxiliaryInfo

/**
  * 没有附加信息
  *
  * @param data 输出数据
  */
class UnaryOutput(override val data: DataFrame) extends PretreatmentOutput(data) {

  override def getData: DataFrame = data
}


/**
  * 没有附加信息
  *
  * @param data 输出数据
  */
class BinaryOutput(override val data: DataFrame,
                   val other: AuxiliaryInfo) extends PretreatmentOutput(data) {

  override def getData: DataFrame = data
}


/**
  * CategoryInformationFor VectorIndex
  */
class CategoryInfoForVectorIndex(val categoricalFeatures: Map[Int, Map[Double, Int]]) extends AuxiliaryInfo


/**
  * 有附加信息的输出
  *
  * @param data    输出数据
  * @param auxiliaryInfo 其他信息
  */
class WithAuxiliaryInfoOutput(override val data: DataFrame,
                              val auxiliaryInfo: Option[AuxiliaryInfo]) extends PretreatmentOutput(data) {
  override def getData: DataFrame = data // 目前遇到的情况额外信息里面没有其他，DataFrame暂定是这样
}
