package cn.datashoe.sparkBase

object TestSparkAPP extends SparkAPP {
  override def run(): Unit = {
    // 测试下sqlc
    sqlc.createDataFrame(Seq.range(0, 100).map(Tuple1.apply)).show()
  }
}
