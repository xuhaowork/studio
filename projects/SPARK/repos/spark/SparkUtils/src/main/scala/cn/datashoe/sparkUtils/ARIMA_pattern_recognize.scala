package cn.datashoe.sparkUtils

import cn.datashoe.sparkBase.SparkAPP

object ARIMA_pattern_recognize extends SparkAPP {
  override def run(): Unit = {
    sqlc.createDataFrame(Seq.range(0 ,100).map(Tuple1.apply)).show()

    import com.github.fommil.netlib.LAPACK

  }
}
