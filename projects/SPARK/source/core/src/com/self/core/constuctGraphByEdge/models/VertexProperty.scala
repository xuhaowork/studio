package com.self.core.constuctGraphByEdge.models

/**
  * 图计算顶点的属性
  * ----
  *
  * @define vertexType           边类型，对应编码如下:
  *                              0:imsi
  *                              1:内网ip
  *                              2:phoneNumber
  *                              3:外网ip
  *                              4:查找点 ?
  * @define someSetAndGetMethods 一些set和get方法
  */
class VertexProperty extends Serializable {
  private var vertexType: Int = 0
  private var vertexTag: String = ""

  def setVertexType(vertexType: Int): VertexProperty = {
    this.vertexType = vertexType
    this
  }

  def getVertexType: Int = vertexType

  def setVertexTag(vertexTag: String): VertexProperty = {
    this.vertexTag = vertexTag
    this
  }

  def getVertexTag: String = vertexTag

}
