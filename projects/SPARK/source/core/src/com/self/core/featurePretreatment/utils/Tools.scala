package com.self.core.featurePretreatment.utils

import java.sql.Timestamp

import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, VectorUDT}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import org.apache.spark.ml.linalg.{Vector => mlVector}
import org.apache.spark.mllib.linalg.Vectors

/**
  * editor: xuhao
  * date: 2018-05-15 10:30:00
  */
object Tools {
  /**
    *
    * @param colName
    * @param data
    * @param requireExist 如果为true则进行异常判定——列名不存在会抛出异常，否则直接返回false
    * @return
    */
  def columnExists(colName: String, data: DataFrame, requireExist: Boolean): Boolean = {
    val schema = data.schema
    if (schema.fieldNames contains colName)
      true
    else {
      if (requireExist)
        throw new IllegalArgumentException(s"您输入的列名${colName}不存在")
      else
        false
    }
  }

  def columnTypesIn(colName: String, data: DataFrame, types: Array[String], requireIn: Boolean): Boolean = {
    columnExists(colName: String, data: DataFrame, true)
    val typeName = data.schema(colName).dataType.typeName
    if (types.map(_.trim.toLowerCase()) contains typeName)
      true
    else {
      if (requireIn)
        throw new IllegalArgumentException(s"${colName}的类型为$typeName，而该算法只支持类型：${types.mkString(",")}.")
      else
        false
    }
  }


  /** 更精确的类型匹配 */
  def columnTypesIn(colName: String, data: DataFrame, requireIn: Boolean, types: DataType*): Boolean = {
    columnExists(colName: String, data: DataFrame, true)
    val typeName = data.schema(colName).dataType
    if (types contains typeName)
      true
    else {
      if (requireIn)
        throw new IllegalArgumentException(s"${colName}的类型为${typeName.simpleString}，而该算法只支持类型" +
          s"：${types.map(_.simpleString).mkString(",")}. 如下提示可能是您需要的：" +
          s"1）请确认您的数据含义正确; " +
          s"2)数据建模中的类型转换或者特征预处理的特征选择中的向量集成、向量拆分、数组集成、数组拆分等可能帮到您。")
      else
        false
    }
  }


  def requireVectorSameSize(colName: String, data: DataFrame): Boolean = {
    columnTypesIn(colName, data, true, new VectorUDT)
    data.select(colName).rdd.map { case Row(v: Vector) => v.size }.reduce {
      case (size1, size2) =>
        if (size1 != size2)
          throw new SparkException("向量长度不一致")
        else
          size1
    }
    true
  }

  def requireVectorShorterThan(colName: String,
                               data: DataFrame,
                               Length: Int,
                               requireSameSize: Boolean): Boolean = {
    columnTypesIn(colName, data, Array("vector"), true)
    data.select(colName).rdd.map { case Row(v: Vector) => v.size }.reduce {
      case (size1, size2) =>
        if (requireSameSize) {
          if (size1 != size2)
            throw new SparkException("向量长度不相等")
          else {
            require(size1 < Length, throw new SparkException(s"向量长度超过$Length"))
            size1
          }
        } else {
          val maxSize = scala.math.max(size1, size2)
          require(maxSize < Length, throw new SparkException(s"向量长度超过$Length"))
          maxSize
        }
    }
    true
  }

  def transform2DenseVector(colName: String,
                            data: DataFrame): DataFrame = {
    import org.apache.spark.sql.NullableFunctions
    import org.apache.spark.sql.functions.col

    columnTypesIn(colName, data, true, new VectorUDT)
    val transfUDF = NullableFunctions.udf((v: Vector) =>
      v match {
        case sv: SparseVector => sv.toDense
        case dv: DenseVector => dv
        case _ => throw new Exception("没有识别到vector类型")
      })

    data.withColumn(colName, transfUDF(col(colName)))
  }

  def getAnyAsArrayDouble[T <: Any](some: Any): Array[Double] = {
    some match {
      case arr: Array[T] => arr.map(_.toString.toDouble)
      case v: Vector => v.toArray
      case others => try {
        Array(others.toString.toDouble)
      } catch {
        case e: Exception =>
          throw new Exception(s"您输入与的类型无法转为数值型，${e.getMessage}")
      }
    }
  }

  /**
    * 确定新加入了的列名合规
    * ----
    * 检查：
    * 1）不能包含特殊字符
    * 2）长度不能超过100个字符
    * 3）不能和已有列名重名
    *
    * @param colName 新加入的列名
    * @param data    数据
    * @return 如果合规返回true否则会抛出对应异常
    */
  def validNewColName(colName: String, data: DataFrame): Boolean = {
    val specialCharacter = Seq(
      '(', ')', '（', '）', '`', ''', '"', '。',
      ';', '.', ',', '%', '^', '$', '{', '}',
      '[', ']', '/', '\\', '+', '-', '*', '、',
      ':', '<', '>')
    val validCharacter = Seq('_', '#')
    specialCharacter.foreach {
      char =>
        require(!(colName contains char), s"您输入的列名${colName}中包含特殊字符：$char," +
          s" 列名信息中不能包含以下特殊字符：${specialCharacter.mkString(",")}," +
          s" 如果您需要用特殊字符标识递进关系，您可以使用一下特殊字符：${validCharacter.mkString(",")}")
    }

    require(!Tools.columnExists(colName, data, false), s"您输入的列名${colName}在" +
      s"数据列名${data.schema.fieldNames.mkString(",")}中已有同名列")
    true
  }


  /**
    * 单组类型的判断
    * 只支持Double、String、Int、Float、Boolean、Long、Short、TimeStamp类型
    * 输出类型的泛型只能是String或Double
    **/
  private def getAsString(vv: Any): String = {
    vv match {
      case d: Double => try {
        d.toString
      } catch {
        case _: Exception => throw new SparkException(s"${d.getClass.getName}数据${d}不能转为对应类型")
      }
      case s: String => try {
        s.toString
      } catch {
        case _: Exception => throw new SparkException(s"${s.getClass.getName}数据${s}不能转为对应类型")
      }
      case i: Int => try {
        i.toString
      } catch {
        case _: Exception => throw new SparkException(s"${i.getClass.getName}数据${i}不能转为对应类型")
      }
      case f: Float => try {
        f.toString
      } catch {
        case _: Exception => throw new SparkException(s"${f.getClass.getName}数据${f}不能转为对应类型")
      }
      case l: Long => try {
        l.toString
      } catch {
        case _: Exception => throw new SparkException(s"${l.getClass.getName}数据${l}不能转为对应类型")
      }
      case bl: Boolean => try {
        bl.toString
      } catch {
        case _: Exception => throw new SparkException(s"${bl.getClass.getName}数据${bl}不能转为对应类型")
      }
      case st: Short => try {
        st.toString
      } catch {
        case _: Exception => throw new SparkException(s"${st.getClass.getName}数据${st}不能转为对应类型")
      }
      case ts: Timestamp => try {
        ts.toString
      } catch {
        case _: Exception => throw new SparkException(s"${ts.getClass.getName}数据${ts}不能转为对应类型")
      }
      case others => throw new SparkException(s"目前不支持${others.getClass.getName}类型的数据$others")
    }
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


  private def assembleAsString(vv: Any*): Seq[String] = {
    val values = mutable.ArrayBuilder.make[String]() // 使用可变对象节约GC
    vv.foreach {
      case d: Double =>
        values += getAsString(d)
      case d: Int =>
        values += getAsString(d)
      case d: Long =>
        values += getAsString(d)
      case d: String =>
        values += getAsString(d)
      case d: Float =>
        values += getAsString(d)
      case d: Boolean =>
        values += getAsString(d)
      case d: Short =>
        values += getAsString(d)
      case d: Timestamp =>
        values += getAsString(d)
      case dv: DenseVector =>
        dv.values.foreach(d => values += d.toString)
      case sv: SparseVector =>
        sv.toDense.values.foreach(d => values += d.toString)
      case mlSv: mlVector =>
        Vectors.fromML(mlSv) match {
          case dv: DenseVector =>
            dv.values.foreach(d => values += d.toString)
          case sv: SparseVector =>
            sv.toDense.values.foreach(d => values += d.toString)
        }
      case arr: Array[Any] =>
        arr.foreach(any => {
          val value: String = try {
            getAsString(any)
          } catch {
            case _: Exception => throw new Exception(s"数组中出现了${any.getClass.getName}类型的元素$any, " +
              s"无法转为double或string构成一层嵌套数组")
          }
          values += value
        })
      case null => throw new Exception("null值无法集成到数组或向量中国")
      case others =>
        throw new Exception(s"暂不支持${others.getClass.getName}类型的数据$others")
    }
    values.result()
  }

  private def assembleAsDouble(vv: Any*): Seq[Double] = {
    val values = mutable.ArrayBuilder.make[Double]() // 使用可变对象节约GC
    vv.foreach {
      case d: Double =>
        values += getAsDouble(d)
      case d: Int =>
        values += getAsDouble(d)
      case d: Long =>
        values += getAsDouble(d)
      case d: String =>
        values += getAsDouble(d)
      case d: Float =>
        values += getAsDouble(d)
      case d: Boolean =>
        values += getAsDouble(d)
      case d: Short =>
        values += getAsDouble(d)
      case d: Timestamp =>
        values += getAsDouble(d)
      case dv: DenseVector =>
        dv.values.foreach(d => values += d)
      case sv: SparseVector =>
        sv.toDense.values.foreach(d => values += d)
      case arr: Array[Any] =>
        arr.foreach(any => {
          val value: Double = try {
            getAsDouble(any)
          } catch {
            case _: Exception => throw new Exception(s"数组中出现了${any.getClass.getName}类型的元素$any, " +
              s"无法转为double或string构成一层嵌套数组")
          }
          values += value
        })
      case others =>
        throw new Exception(s"暂不支持${others.getClass.getName}类型的数据$others")
    }
    values.result()
  }

  def getTheUdfByType(typeName: String): UserDefinedFunction =
    typeName.toLowerCase match {
      case "string" => udf { r: Row =>
        assembleAsString(r.toSeq: _*)
      }
      case "double" => udf { r: Row =>
        assembleAsDouble(r.toSeq: _*)
      }
      case _ => throw new Exception(s"暂不支持${typeName}类型")
    }


}
