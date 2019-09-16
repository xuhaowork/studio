package com.self.core.learningLR

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.Vectors

object LearningLR extends myAPP {
  override def run(): Unit = {

    /**
      * where \delta_{i, j} = 1 if i == j,
      * \delta_{i, j} = 0 if i != j, and
      * multiplier =
      * \exp(margins_i) / (1 + \sum_k^{K-1} \exp(margins_i)) - (1-\alpha(y)\delta_{y, i+1})
      *
      */

    println(3.0 / 2 - 5.0 / 2 * 5.0 / 2)


    println(1.5 - 6.25)

    println(
      """fff"""
        +
        """fff""")


    println(24 * 3600)



    val data = Seq(
      (-1.0, Vectors.dense(Array(1.0, 3.0, 5.0)), 11F, Vectors.dense(Array(1.0, 2.0, -0.0))),
      (0.0, Vectors.dense(Array(1.0)), 13F, Vectors.dense(Array(1.0, 2.0, -0.0))),
      (2.0, Vectors.sparse(6, Array(1, 3), Array(0.0, 1.0)), 15F, Vectors.dense(Array(1.0, 2.0, -0.0)))
    )

    val dataFrame = sqlc.createDataFrame(data).toDF("features", "notEqualLengthVector", "floatType","equalDense")

    import org.apache.spark.mllib.linalg.VectorUDT
    println(dataFrame.select("equalDense").schema.head.dataType)


    import org.apache.spark.mllib.tree.RandomForest

    import org.apache.spark.mllib.tree.RandomForest
    import org.apache.spark.mllib.tree.model.RandomForestModel

  val u = Array.range(0, 10).map(_ => 2)








  }
}
