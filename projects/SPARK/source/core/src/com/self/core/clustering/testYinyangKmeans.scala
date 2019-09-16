package com.self.core.clustering

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.Vectors

object testYinyangKmeans extends myAPP{
  override def run(): Unit = {
    val km = new yinyangKmeans()

    val path = "E:/scala-spark/data/Wholesale customers data.csv"
    val text = sc.textFile(path)

    val header = text.take(1).head
    val rdd = text.filter(!_.equals(header))
      .map(_.split(",", header.split(",").length))
      .map(x => Vectors.dense(x.map(_.trim.toDouble))).cache()


    val model = km.setK(3).run(rdd)
    model.clusterCenters.foreach(println)









  }
}
