package com.self.core.BMJL.distCompute

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.clustering.KMeans

/**
  * Created by DataShoe on 2018/1/10.
  * 判断相似度的方案：
  * 1)plan1, 时间分箱 => 聚合&特征提取 => 数据标准化 => 基于基本的距离函数判断距离。
  * 2)plan2, 特征提取 => 将其转化为一系列的时间序列 => 基于序列相似性判断距离。
  * plan1侧重于两者行为在同一时间上的协同性, plan2侧重于两者在行为上的相似性。
  * 最后两者可以取其一或者通过加权求和的方式兼顾。
  */


object DistCompute extends myAPP{
  override def run(): Unit = {
//    // 数据源
//    val path = "G://网络行为分析数据/ipsession.csv"
//    val header = "true"
//    var rawDataDF = sqlc.read.format("com.databricks.spark.csv")
//      .option("header", header).option("inferSchema", true.toString).load(path)
//    rawDataDF = rawDataDF.withColumnRenamed(rawDataDF.columns(0), "DRETBEGINTIME")
//
//    rawDataDF.show()
//
//    rawDataDF.except(rawDataDF)
//
//    rawDataDF.groupBy("a").count().alias("g")


    val u = Array(1, 2, 4, 6, 8, 10, 12).sliding(3).map(x => x.mkString(", ")).toArray
    u.foreach(println)




//    org.apache.spark.ml.feature.Bucketizer

    org.apache.spark.sql.Row























  }
}
