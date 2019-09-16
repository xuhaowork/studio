package cn.datashoe.dataWrangling

import cn.datashoe.sparkUtils.DataSimulate
import org.scalatest.FunSuite

class DiscretizationSuite extends FunSuite {
  test("测试基于误差范围的离散化") {
    import DiscretizationByMeansIter._

    val colName = "薪资"
    val delta = 1000.0
    val outputCol = "outputCol"

    val seed = 1123L

    val data = DataSimulate.salaryData(20, 1, Some(1123L))
    data.show()

    val centers = train(data, colName, delta, seed)
    println(centers.mkString(", "))

    val newDataFrame = predict(data, colName, centers, outputCol)

    newDataFrame.show(200)
  }


  test("求最近的中心") {
    val data = DataSimulate.sqlc.createDataFrame(Seq(
      1.0, 5, 6, 8, 10, 20, 70
    ).map(Tuple1.apply)).toDF("value")
    val centers = Array(2.0, 5.1, 17)
    val res = DiscretizationByMeansIter.predict(data, "value", centers, "outputCol")
    res.show()
    val mp = res.select("value", "outputCol")
      .rdd
      .map(row => (row.getAs[Double](0), row.getAs[Double](1)))
      .collectAsMap()

    val shouldBe = Map(
      (1, 2.0),
      (5, 5.1),
      (6, 5.1),
      (8, 5.1),
      (10, 5.1),
      (20, 17.0),
      (70, 17.0)
    )

    require(mp == shouldBe)
    println(mp == shouldBe)
  }

}
