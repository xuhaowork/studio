package cn.datashoe.dataWrangling

/**
  * 数量尺度变换
  * ----
  * 数据的尺度变换工具类: 就是最基本的数据元素进行加减乘除, 使得原有数据在尺度和位置上发生统一的转换
  * ----
  * 1)z-score
  * 2)min-max
  * 3)log
  * 4)Box-Cox
  * 5)atan
  * 6)sigmoid
  * 7)sinh|cosh|tanh等双曲三角函数
  * 8)非连续线性变换
  * 9)直角坐标转极坐标
  */
object ScaleTransformer extends Serializable {
  /**
    * z-score变换
    *
    * @param value 数据
    * @param mean  均值
    * @param std   方差 方差大于等于0.0, 当等于0.0时所有数据一样, 此时变换直接返回0.0
    * @return
    */
  def z_score(value: Double, mean: Double, std: Double): Double =
    if (std < 0.0)
      throw new IllegalArgumentException(s"您输入的标准差'$std'小于0, 不符合标准差的定义")
    else if (std == 0.0)
      value - mean
    else
      (value - mean) / std

  /**
    *
    * @param value     数据
    * @param min       最小值
    * @param deviation 离差 离差大于等于0.0, 当等于0.0时所有数据一样, 此时变换直接返回0.0
    * @return
    */
  def min_max(value: Double, min: Double, deviation: Double): Double =
    if (deviation < 0.0)
      throw new IllegalArgumentException(s"您输入的离差(最大值最小值之差)为'$deviation', 最大值比最小值小")
    else if (deviation == 0.0)
      0.0
    else
      (value - min) / deviation

  /**
    * 对数变换
    *
    * @param value 当大于0.0时返回对应值, 当小于0.0时返回Double.NaN, 等于0.0时返回+-Infinity(+-取决于底数和1的大小关系)
    * @param base  需要大于0.0且不等于1.0
    * @return
    */
  def log(value: Double, base: Double = scala.math.E, plus: Double = 0.0): Double =
    if (base == scala.math.E)
      scala.math.log(value + plus)
    else {
      require(base > 0.0 && base != 1.0, s"底数需要大于0且不能为1, 您的底数为'$base'")
      scala.math.log(value + plus) / scala.math.log(base)
    }


  /**
    * Box-Cox变换
    * ----
    * equation: ``` \frac{(x + plus) ^ {\lambda} - 1}{\lambda} ```
    * ----
    *
    * @param value  数据
    * @param lambda lambda(指数项)
    * @param plus   最小加和项, 确保value + plus大于0
    * @return 当value + plus > 0时, NaN, 否则box-cox变换
    */
  def box_cox(value: Double, lambda: Double, plus: Double = 0.0): Double =
    if (value + plus <= 0.0) {
      Double.NaN
    } else {
      if (lambda == 0.0)
        log(value + plus)
      else {
        (scala.math.pow(value + plus, lambda) - 1) / lambda
      }
    }

  /**
    * arc-tan变换
    *
    * @param value 实数
    * @return
    */
  def atan(value: Double): Double = scala.math.atan(value)

  /**
    * 双曲余弦
    */
  def cosh(value: Double): Double = scala.math.sin(value)

  /**
    * 双曲正切
    */

  def tanh(value: Double): Double = scala.math.tanh(value)

  /**
    * 双曲正弦
    */
  def sinh(value: Double): Double = scala.math.sinh(value)

  /**
    * sigmoid函数
    */
  def sigmoid(value: Double): Double = scala.math.tanh(value / 2) / 2 + 0.5

  /**
    * 频率非连续线性变换
    * ----
    * 功能：给空值和0值更多的惩罚，给非0值增加奖励。防止两个点因为过多的空值而产生的相似.
    * -- 注意这里提供的是单个元素的变换
    * ----
    * 例如:
    * v1 = [0.01, null, null, 0.1]
    * v2 = [null, 0.01, 0.01, null]
    * v3 = [0.03, null, null, 0.08]
    * 在欧式距离中会把null作为0.0进行处理，因此v1和v2相近，而业务中null除了代表频率为0之外，还代表属性的缺失，因此需要进行变换
    * ----
    * 变换: x => (1 - alpha) * x * signum(x) + alpha --前半部分表示对频率权重，后半部分对发生这一属性的重视
    * 此时当x为null或0.0时，认为是0.0，大于0.0是进行一个变换。该比那换是非连续的，一次增加空值的惩罚。
    * 极端情况 1）当alpha等于0时，测试没有任何操作
    * 2）当alpha等于1时，此时是纯one-hot编码，发生时1.0，不发生是0.0。
    * 此时v1 = [1, 0, 0, 1]，v2 = [0, 1, 1, 0]，v3 = [1, 0, 0, 1]，显然v1和v3更接近，此时给予空值更多的惩罚。
    * ----
    *
    * @param frequency 输入的频率，频率需要大于等于0.0小于等1.0才有意义.
    * @param alpha     输入的非连续映射系数，需要在0和1之间才有意义.
    */
  def sigRectify(frequency: Double, alpha: Double): Double =
    scala.math.signum(frequency) * (1 - alpha) + alpha


  /**
    * 直角坐标转极坐标
    *
    * @param coordinate 直角坐标 二维
    * @return 极坐标 (r, theta)
    */
  def coordinates2polar(coordinate: (Double, Double)): Double = {
    scala.math.atan2(coordinate._1, coordinate._2)
  }  // todo: 暂时放在这里

}
