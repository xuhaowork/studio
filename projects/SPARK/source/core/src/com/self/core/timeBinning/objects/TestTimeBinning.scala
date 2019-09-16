package com.self.core.timeBinning.objects

import com.self.core.baseApp.myAPP
import com.self.core.timeBinning.models.TestData
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

object TestTimeBinning extends myAPP{
  def simulate() = {
    val testData = TestData.testData1
    import TestData.ArrayToMatrix
    val lst = testData.toMatrixArrayByRow(20, 3)

    val rdd = sc.parallelize(lst).map(Row.fromSeq(_))
    val rawDataDF = sqlc.createDataFrame(rdd, StructType(Array(StructField("time", StringType),
      StructField("id", IntegerType), StructField("age", IntegerType))))

    memoryMap.put("rawDataDF", rawDataDF)
  }
  simulate()

  override def run(): Unit = {
    val rawDataDF = memoryMap.get("rawDataDF").asInstanceOf[DataFrame]
    rawDataDF.show()
  }

}
