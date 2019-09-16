package com.self.core.constuctGraphByEdge.models

/**
  * 图计算中的边属性
  * ----
  * 包含以下内容：
  *
  * @define edgeType             边类型：Int, 具体编码对应如下：
  *                              0:ip to ip;
  *                              1:imsi use ip;
  *                              2:imsi access ip;
  *                              3:ip access imsi;
  *                              4:imsi call phone;
  *                              5:phone call imsi;
  *                              6:phone call phone
  * @define tag                  唯一的记录标识符
  * @define someSetAndGetMethods 一些set和get方法
  * @define whoIsEarlier         比较谁的id小，取id较小的一个值
  */
class EdgeProperty extends Serializable {
  private var tag: Long = 0L
  private var edgeType: Int = 0
  private var frequency: Long = 1L
  private var edgeTag: String = ""

  def setTag(tag: Long): this.type = {
    this.tag = tag
    this
  }

  def setEdgeType(edgeType: Int): this.type = {
    this.edgeType = edgeType
    this
  }

  def setFrequency(frequency: Long): this.type = {
    this.frequency = frequency
    this
  }

  def setEdgeTag(edgeTag: String): this.type = {
    this.edgeTag = edgeTag
    this
  }

  def getTag: Long = tag

  def getEdgeType: Int = edgeType

  def getFrequency: Long = frequency

  def getEdgeTag: String = edgeTag

  def add(other: EdgeProperty): EdgeProperty = {
    require(edgeType == other.edgeType, "边的类型必须一致频率才能合并相加。")
    new EdgeProperty().setTag(tag).setEdgeType(edgeType).setFrequency(frequency + other.frequency)
  }

}
