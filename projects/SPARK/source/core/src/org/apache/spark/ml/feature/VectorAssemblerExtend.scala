package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute, UnresolvedAttribute}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/**
  * editor：Xuhao
  * date： 2018-07-05 10:00:00
  */

/**
  * 扩展了[[VectorAssembler]]使其能支持一维嵌套数组的集成
  * 一维嵌套数组：目前只支持数组中的元素为string或double类型，string类型时要求是数值
  */
class VectorAssemblerExtend extends VectorAssembler {
  override def transform(dataset: DataFrame): DataFrame = {
    val schema = dataset.schema
    lazy val first = try {
      dataset.first()
    } catch {
      case e: Exception => throw new Exception(s"获取第一条数据时失败，e${e.getMessage}")
    }

    val attrs = $(inputCols).flatMap { c =>
      val field = schema(c)
      val index = schema.fieldIndex(c)
      field.dataType match {
        case DoubleType =>
          val attr = Attribute.fromStructField(field)
          // If the input column doesn't have ML attribute, assume numeric.
          if (attr == UnresolvedAttribute) {
            Some(NumericAttribute.defaultAttr.withName(c))
          } else {
            Some(attr.withName(c))
          }
        case _: NumericType | BooleanType =>
          // If the input column type is a compatible scalar type, assume numeric.
          Some(NumericAttribute.defaultAttr.withName(c))
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(field)
          if (group.attributes.isDefined) {
            // If attributes are defined, copy them with updated names.
            group.attributes.get.map { attr =>
              if (attr.name.isDefined) {
                // TODO: Define a rigorous naming scheme.
                attr.withName(c + "_" + attr.name.get)
              } else {
                attr
              }
            }
          } else {
            // Otherwise, treat all attributes as numeric. If we cannot get the number of attributes
            // from metadata, check the first row.
            val numAttrs = group.numAttributes.getOrElse(first.getAs[Vector](index).size)
            Array.fill(numAttrs)(NumericAttribute.defaultAttr)
          }
        case at: ArrayType =>
          require(at.elementType == StringType || at.elementType == DoubleType,
            s"数组需要为一层嵌套数组，并且其中元素类型只能是string或double，" +
              s"而您的类型为${at.elementType.simpleString}")
          Array.fill(first.getAs[Seq[Any]](index).length)(NumericAttribute.defaultAttr)
        case otherType =>
          throw new SparkException(s"VectorAssembler does not support the $otherType type")
      }
    }
    val metadata = new AttributeGroup($(outputCol), attrs).toMetadata()

    // Data transformation.
    val assembleFunc = udf { r: Row =>
      VectorAssemblerExtend.assemble(r.toSeq: _*)
    }
    val args = $(inputCols).map { c =>
      schema(c).dataType match {
        case ArrayType(StringType, _) => dataset(c)
        case ArrayType(DoubleType, _) => dataset(c)
        case DoubleType => dataset(c)
        case _: VectorUDT => dataset(c)
        case _: NumericType | BooleanType => dataset(c).cast(DoubleType).as(s"${c}_double_$uid")
      }
    }

    dataset.select(col("*"), assembleFunc(struct(args: _*)).as($(outputCol), metadata))
  }

}


object VectorAssemblerExtend {
  def assemble(vv: Any*): Vector = {
    val indices = mutable.ArrayBuilder.make[Int]
    val values = mutable.ArrayBuilder.make[Double]
    var cur = 0
    vv.foreach {
      case v: Double =>
        if (v != 0.0) {
          indices += cur
          values += v
        }
        cur += 1
      case vec: Vector =>
        vec.foreachActive { case (i, v) =>
          if (v != 0.0) {
            indices += cur + i
            values += v
          }
        }
        cur += vec.size
      case arr: Seq[Any] =>
        arr.foreach { element =>
          indices += cur
          values += getAsDouble(element)
          cur += 1
        }
      case null =>
        // TODO: output Double.NaN?
        throw new SparkException("null值不能被集成到向量中")
      case o =>
        throw new SparkException(s"$o of type ${o.getClass.getName} 不能被集成到向量中")
    }
    Vectors.sparse(cur, indices.result(), values.result()).compressed
  }


  private def getAsDouble(vv: Any): Double = {
    vv match {
      case d: Double => try {
        d.toDouble
      } catch {
        case _: Exception => throw new SparkException(s"${d.getClass.getName}数据${d}不能转为对应类型")
      }
      case s: String => try {
        s.toDouble
      } catch {
        case _: Exception => throw new SparkException(s"${s.getClass.getName}数据${s}不能转为对应类型")
      }
      case i: Int => try {
        i.toDouble
      } catch {
        case _: Exception => throw new SparkException(s"${i.getClass.getName}数据${i}不能转为对应类型")
      }
      case f: Float => try {
        f.toDouble
      } catch {
        case _: Exception => throw new SparkException(s"${f.getClass.getName}数据${f}不能转为对应类型")
      }
      case l: Long => try {
        l.toDouble
      } catch {
        case _: Exception => throw new SparkException(s"${l.getClass.getName}数据${l}不能转为对应类型")
      }
      case bl: Boolean => try {
        if (bl) 1.0 else 0.0
      } catch {
        case _: Exception => throw new SparkException(s"${bl.getClass.getName}数据${bl}不能转为对应类型")
      }
      case st: Short => try {
        st.toDouble
      } catch {
        case _: Exception => throw new SparkException(s"${st.getClass.getName}数据${st}不能转为对应类型")
      }
      case others => throw new SparkException(s"目前不支持${others.getClass.getName}类型的数据$others")
    }
  }


}
