package com.self.core.syntax

import org.apache.spark.rdd.RDD

class learningPrivate private(private var height: Double,
                              private var weight: Double,
                              private var name: String) extends Serializable {
  def this() = this(171.5, 75.5, "DataShoe")
  def set_name(name: String): this.type = {
    this.name = name
    this
  }

  def set_height(height: Double): this.type = {
    this.height = height
    this
  }

  def set_weight(weight: Double): this.type = {
    this.weight = weight
    this
  }

  def changRdd(rdd: RDD[(Double, Double, String)])
  : RDD[(Double, Double, String)] = {
    rdd.map(_ => (height, weight, name))
  }


}

object learningPrivate{
  def train(name: String, height: Double, weight: Double, rdd: RDD[(Double, Double, String)])
  : RDD[(Double, Double, String)] = {
    val newRdd = new learningPrivate()
      .set_height(height)
      .set_weight(weight)
      .set_name(name).changRdd(rdd)
    newRdd
  }
}
