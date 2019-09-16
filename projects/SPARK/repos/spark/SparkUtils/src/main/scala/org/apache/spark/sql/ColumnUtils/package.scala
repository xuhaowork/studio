package org.apache.spark.sql


import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
package object ColumnUtils {
  object ColumnImpl {

    /**
      * 列名处理
      *
      * @param colName 列名
      */
    implicit class ColNameImpl(colName: String) {
      // 列名是否在DataFrame中
      def in(dataFrame: DataFrame): Boolean = {
        dataFrame.schema.fieldNames contains colName
      }

      // 列名是否合法 可以自定义规则 暂无
      def isValid: Boolean = {
        true
      }

      def fieldOf(dataFrame: DataFrame): StructField = {
        require(colName in dataFrame, s"列名${colName}不在数据表中.")
        dataFrame.schema(colName)
      }

      def dataTypeIn(dataFrame: DataFrame)(dataTypes: String*): Boolean = {
        val dataType = fieldOf(dataFrame).dataType

        dataTypes.exists {
          name =>
            name.toLowerCase match {
              case "fractional" =>
                dataType.isInstanceOf[FractionalType]
              case "integral" =>
                IntegralType.acceptsType(dataType)
              case "numeric" =>
                NumericType.acceptsType(dataType)
              case "atomic" =>
                dataType.isInstanceOf[AtomicType]
              case _ =>
                dataType.sameType(CatalystSqlParser.parseDataType(name))
            }
        }
      }

    }





  }


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

  def cast2Numeric(num: String, lowerBound: Double = Double.MinValue, closedInterval: Boolean = true): Double = {
    val value = try {
      num.toString.toDouble
    } catch {
      case _: Exception => throw new Exception(s"在将'$num'转为数值类型时失败。")
    }
    require(
      if (closedInterval) value >= lowerBound else value > lowerBound,
      s"需要数值大于${if (closedInterval) "等于" else ""}数值${lowerBound}但现在数值为：$num"
    )
    value
  }





}
