package org.apache.spark.sql.types

import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.catalyst.SqlLexical

/**
  * editor：Xuhao
  * date： 2018-07-04 10:00:00
  */

/**
  * 一个新的类型解析parser
  * ----
  * 这里解析字符串要求是符合一定规格的：一般是DataType对应的simpleString值
  * 抽象类(如NumericType、AtomicType等)无法实例化因此无法解析为具体DataType
  * ----
  *
  * 1）DataTypeParser不支持vector类型的解析(@see [[DataTypeParser]])，这里加入支持
  * 2）另外加入中文类的报错信息 @define [[failMessage]]
  */
class NewDataTypeParser extends DataTypeParser {
  /** vector类型 */
  lazy val vectorType: Parser[DataType] =
    "(?i)vector".r ^^^ new VectorUDT

  override lazy val dataType: Parser[DataType] =
    arrayType |
      mapType |
      structType |
      primitiveType |
      vectorType

  override val lexical = new SqlLexical

  override def toDataType(dataTypeString: String): DataType = synchronized {
    phrase(dataType)(new lexical.Scanner(dataTypeString)) match {
      case Success(result, _) => result
      case _: NoSuccess => throw new DataTypeException(failMessage(dataTypeString))
    }
  }

  private def failMessage(dataTypeString: String): String = {
    s"没有成功解析到您输入的类型名称: $dataTypeString，首先您确认是否含有不应有特殊字符或中文字符；" +
      s" 其次如果要解析的名称是一个struct而其中有field列名包含特殊字符请以反引号'`'将其括起来。"
  }
}

/** 伴生对象 */
object NewDataTypeParser {
  def parse(dataTypeString: String): DataType =
    (new NewDataTypeParser).toDataType(dataTypeString)
}
