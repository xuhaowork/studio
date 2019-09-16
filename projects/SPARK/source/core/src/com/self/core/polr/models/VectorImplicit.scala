package com.self.core.polr.models

object VectorImplicit extends Serializable {
  import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}

  implicit class VectorAppendImplicit(val values: Vector){
    def append(appender: Vector): Vector =
      (values, appender) match {
        case (v1: DenseVector, v2: DenseVector) =>
          new DenseVector(v1.values ++ v2.values)
        case (v1: DenseVector, v2: SparseVector) =>
          new DenseVector(v1.values ++ v2.toDense.values)

        case (v1: SparseVector, v2: DenseVector) =>
          new SparseVector(
            v1.size + v2.size,
            v1.indices ++ Array.tabulate(v2.size)(i => i + v1.size),
            v1.values ++ v2.values)

        case (v1: SparseVector, v2: SparseVector) =>
          new SparseVector(
            v1.size + v2.size,
            v1.indices ++ v2.indices.map(_ + v1.size),
            v1.values ++ v2.values)
      }

    def append(appender: Array[Double]): Vector =
      values match {
        case v1: DenseVector =>
          new DenseVector(v1.values ++ appender)
        case v1: SparseVector =>
          new SparseVector(
            v1.size + appender.length,
            v1.indices ++ Array.tabulate(appender.length)(i => i + v1.size),
            v1.values ++ appender)
      }

  }

  implicit class VectorLastImplicit(val values: Vector) {
    def first(num: Int): Array[Double] = values match {
      case v1: DenseVector =>
        v1.values.take(num)
      case v1: SparseVector =>
        v1.indices.take(num).map(v1.values.apply)
    }
    def last(num: Int): Array[Double] = values match {
      case v1: DenseVector =>
        v1.values.takeRight(num)
      case v1: SparseVector =>
        v1.indices.takeRight(num).map(v1.values.apply)
    }
  }


  implicit class VectorDropImplicit(val values: Vector) {
    def dropRight(num: Int): Vector = values match {
      case v1: DenseVector => new DenseVector(v1.values.dropRight(num))
      case v1: SparseVector => new SparseVector(
        v1.size - num, v1.indices.dropRight(num), v1.values.dropRight(num))
    }

    def drop(num: Int): Vector = values match {
      case v1: DenseVector => new DenseVector(v1.values.drop(num))
      case v1: SparseVector => new SparseVector(
        v1.size - num, v1.indices.drop(num), v1.values.drop(num))
    }
  }

}
