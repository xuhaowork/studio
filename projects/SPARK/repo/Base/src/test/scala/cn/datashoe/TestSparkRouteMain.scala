package cn.datashoe

import org.scalatest.FunSuite

class TestSparkRouteMain extends FunSuite {
  test("本地运行Spark Main基类") {
    object Abj extends SparkRouteMain {
      override def run(): Unit = {
        val rd = new java.util.Random(1123L)
        val input = sqlc.createDataFrame(Seq.fill(100){
          val mod = rd.nextInt() % 10
          (if(mod < 0) -mod else mod, rd.nextGaussian())
        }).toDF("Id", "value")

        input.show(false)

        input.groupBy("Id").sum("value").as("sum").show()

        println("good")

      }
    }

    Abj.run()
  }

}
