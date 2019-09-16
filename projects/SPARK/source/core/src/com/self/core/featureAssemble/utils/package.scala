package com.self.core.featureAssemble

import org.apache.spark.mllib.linalg.SparseVector

/**
  * 特征集成的工具类
  */
package object utils extends Serializable {
  /**
    * 频率非连续线性变换
    * ----
    * 功能：给空值和0值更多的惩罚，给非0值增加奖励。防止两个点因为过多的空值而产生的相似.
    * ----
    * 例如:
    * v1 = [0.01, null, null, 0.1]
    * v2 = [null, 0.01, 0.01, null]
    * v3 = [0.03, null, null, 0.08]
    * 在欧式距离中会把null作为0.0进行处理，因此v1和v2相近，而业务中null除了代表频率为0之外，还代表属性的缺失，因此需要进行变换
    * ----
    * 变换: x => (1 - alpha) * x + alpha --前半部分表示对频率权重，后半部分对发生这一属性的重视
    * 此时当x为null或0.0时，认为是0.0，大于0.0是进行一个变换。该比那换是非连续的，一次增加空值的惩罚。
    * 极端情况 1）当alpha等于0时，测试没有任何操作
    * 2）当alpha等于1时，此时是纯one-hot编码，发生时1.0，不发生是0.0。
    * 此时v1 = [1, 0, 0, 1]，v2 = [0, 1, 1, 0]，v3 = [1, 0, 0, 1]，显然v1和v3更接近，此时给予空值更多的惩罚。
    * ----
    *
    * @param x     输入的x，这里x是每个向量的元素
    * @param alpha 输入的非连续映射系数，需要在0和1之间
    */
  def sigRectify(x: Double, alpha: Double): Double =
    (1 - alpha) * x + alpha

  def sigRectify(x: Int, alpha: Double): Double =
    (1 - alpha) * x + alpha

  def sigRectify(x: Long, alpha: Double): Double =
    (1 - alpha) * x + alpha

  def sigRectify(x: Float, alpha: Double): Double =
    (1 - alpha) * x + alpha

  def sigRectifyVector(data: SparseVector, alpha: Double): SparseVector =
    if (alpha <= 0.0) {
      data
    } else if (alpha >= 1.0) {
      new SparseVector(data.size, data.indices, data.indices.map(_ => 1.0))
    } else {
      new SparseVector(data.size, data.indices, data.values.map(sigRectify(_, alpha)))
    }

}
