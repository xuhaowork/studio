package org.apache.spark.sql

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{AtomicType, _}

import scala.collection.Iterable


/**
  * editor：Xuhao
  * date： 2018-07-04 10:00:00
  */

/**
  * 一个DataFrame中列数据类型处理的工具包
  * ----
  * 用到了新写的类型解析，加入了vector解析 --@see [[NewDataTypeParser]]
  *
  * @define columnExists 判定列名是否存在
  * @define DataTypeImpl 判定类型是否是需要的类型[TypesInImpl]和判定是否是原子类型[IsAtomicImplicit]
  *                      1）可以判定一般的DataType，同时可以判定抽象接口类的DataType，如Numeric、Atomic等等
  *                      2）判定是否是原子类型（即[[AtomicType]]包含的类型），可以用于集合和嵌套类型的区别
  *                      3）他们都是隐式转换类
  */
package object columnUtils {

  /**
    * 判定列名是否存在于DataFrame中
    *
    * @param colName      列名
    * @param data         对应的DataFrame
    * @param requireExist 如果为true则进行异常判定——列名不存在会抛出异常，否则直接返回false
    * @return 是否存在于DataFrame，是一个Boolean类型
    */
  def columnExists(colName: String, data: DataFrame, requireExist: Boolean = true): Boolean = {
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


  object DataTypeImpl {

    /**
      * 是否是要求类型的隐含类
      * ----
      * 示例：
      * val dt = df.schema("id").dataType
      * import DataTypeImpl._
      * dt in Seq("string", "int", "decimal(20, 0)") // 类型在string/int中
      * dt in Seq("numeric", "vector", "array<double>") // 类型为numeric类/vector类/array<double>类
      * dt in Seq("atomic") // 类型为原子类 --具体类型参见接口[[AtomicType]]
      * dt in Seq("integral") // 类型为整数型 --具体类型参见接口[[IntegralType]]
      * dt.in(Seq("timestamp", "tinyint", "smallint", "bigint",
      * "varchar", "decimal", "float"), true)
      * df.schema.in(Seq("struct<value:int,square:int,value_decimal:decimal(10,0)>"),true)
      * ----
      *
      * @define in 是否在要求的类型中
      *            --这里要求的类型为String类型，是为了给非sql目录下的代码提供更多支持
      *            --这里类型需要满足一定的格式才能被解析到，一般是DataType的simpleString
      *            --这里加入了对抽象数据类型的判定，因此可以使用一种接口的
      *            --当withException为true时会抛出异常
      */
    // @todo struct的解析目前还不支持具体到nullable的处理，--struct带nullable是默认为true
    implicit class TypesInImpl(val dataType: DataType) {
      def in(types: Iterable[String], withException: Boolean = false): Boolean =
        if (withException) {
          val isExists = types.exists(typeName => dataType in typeName)
          if (!isExists)
            throw new DataTypeException(s"您输入的类型为${dataType.simpleString}, 而要求类型为${types.mkString(",")}")
          else
            true
        } else {
          types.exists(typeName => dataType in typeName)
        }


      def in(types: String): Boolean =
        types.toLowerCase match {
          case name if name contains "fractional" =>
            dataType.isInstanceOf[FractionalType]
          case name if name contains "integral" =>
            dataType.isInstanceOf[IntegralType]
          case name if name contains "numeric" =>
            NumericType.acceptsType(dataType)
          case name if name contains "atomic" =>
            dataType.isAtomicType
          case name: String =>
            dataType == NewDataTypeParser.parse(name)
        }

    }

    /**
      * 是否是原子类型和是否是数组嵌套原子类型的隐含类
      *
      * @define isAtomicType      类型是否是原子类型
      * @define isArrayAtomicType 类型是否是数组嵌套原子类型
      * @param dataType 类型
      */
    implicit class IsAtomicImplicit(val dataType: DataType) {
      /**
        * 判定类型是否是原子类型
        *
        * @return 是否是要求的类型，是一个Boolean类型
        */
      def isAtomicType: Boolean =
        dataType.isInstanceOf[AtomicType]

      /**
        * 判定数组元素是否是原子类型 --也就是一层嵌套数组
        *
        * @return 返回Boolean类型：是否是一层嵌套数组
        *         如果不是数组类型会抛出异常
        */
      def isArrayAtomicType: Boolean = {
        dataType match {
          case ArrayType(elementType, _) =>
            if (elementType.isAtomicType)
              true
            else
              false
          case _ => throw new DataTypeException("不是array类型")
        }
      }
    }

  }


  /**
    * spark为了迁移代码同时向前兼容导致有两个版本的vector：
    * [[org.apache.spark.mllib.linalg.Vector]]和[[org.apache.spark.ml.linalg.Vector]] --很烦，经常模式不匹配的情况。
    * ----
    * 这里定义了两个转换的udf，直接从数据中将所有vector类型统统转为mllib的vector或ml的vector --具体选择哪个根据个人喜好。
    *
    * @define transform2mllibVector 将所有类型的vector都转为mllib的vector
    * @define transform2mlVector    将所有类型的vector都转为ml的vector
    */
  object VectorTransformation {
    import org.apache.spark.ml.linalg.{Vector => mlVector}
    import org.apache.spark.mllib.linalg.Vectors.fromML
    import org.apache.spark.mllib.linalg.{Vector => mllibVector}
    import org.apache.spark.SparkException


    val transform2mllibVector: UserDefinedFunction = NullableFunctions.udf(
      (vec: Any) => vec match {
        case mllibVec: mllibVector => mllibVec
        case mlVec: mlVector => fromML(mlVec)
        case other =>
          throw new SparkException(s"您输入的数据${other}是${other.getClass.getSimpleName}类型不是mllib或ml中的vector")
      }
    )

    val transform2mlVector: UserDefinedFunction = NullableFunctions.udf(
      (vec: Any) => vec match {
        case mllibVec: mllibVector => mllibVec.asML
        case mlVec: mlVector => mlVec
        case other =>
          throw new SparkException(s"您输入的数据${other}是${other.getClass.getSimpleName}类型不是mllib或ml中的vector")
      }
    )

  }


}
