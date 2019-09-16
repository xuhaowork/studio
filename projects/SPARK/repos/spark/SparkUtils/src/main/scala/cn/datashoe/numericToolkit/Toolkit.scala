package cn.datashoe.numericToolkit

import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * <pre>
  * (1) 2018-12-11 删除了对Json通用解析的支持。
  * (2) 2018-12-10 更新了toSignType方法，以满足不同场景下可以自定义打印消息。并将条件判断封装到了computeNumericType方法中。
  * (3) 2018-12-11 更新了filterNonIntegerRecord方法在类型为ALL时的正则表达式和方法说明文档。
  * (4) 2018-12-11 更新了filterNonNumericRecord方法的说明文档。
  * (5) 2018-12-11 更新了toRangeType方法，让使用者可以根据自己的需求进行调用
  *
  * </pre>
  */
object Toolkit extends Serializable {

  /** ******************************************************************************************************************/
  // DataFrame

  /**
    * 用于检测字段是否存在于指定的DataFrame中。
    *
    * @param df        需要待检测的DataFrame。
    * @param fields    待检测的的字段数组。
    * @param tableName DataFrame的表名，类似于为DataFrame的别名，建议与inputTableName保持一致。
    * @param isTrigger Boolead类型，当为True时，表示只要检测到不存在的字段，立即产生异常。
    *                  当为False时，将所有的字段检测完后再报异常，将不能存在的字段一一列出。
    * @return Unit
    */
  def fieldExistByDataFrame(df: DataFrame, fields: Seq[String], tableName: String, isTrigger: Boolean = false): Unit = {

    val colNames = df.columns
    val noExistsBuffer = new ArrayBuffer[String]()
    fields.foreach(field => {
      val isContains = colNames.contains(field)
      if (isTrigger) {
        assert(isContains, s"FieldNotExistError:检测到字段名称为 $field 的不在 $tableName 的数据字段中!")
      } else {
        if (!isContains) noExistsBuffer.append(field)
      }
    })

    if (noExistsBuffer.nonEmpty) throw new Exception(s"FieldNotExistError:检测下列字段名称 $noExistsBuffer 不在 $tableName 的数据字段中!")
  }

  /**
    * 用于检测字段是否存在于指定的DataFrame中。
    *
    * @param df        需要待检测的DataFrame。
    * @param fields    待检测的的字段数组。
    * @param isTrigger Boolead类型，当为True时，表示只要检测到不存在的字段，立即产生异常。
    *                  当为False时，将所有的字段检测完后再报异常，将不能存在的字段一一列出。
    * @return Unit
    */
  def fieldExistByDataFrame(df: DataFrame, fields: Seq[String], isTrigger: Boolean): Unit = {
    fieldExistByDataFrame(df, fields, "数据集", isTrigger)
  }

  /**
    * 过滤指定DataFrame的null值和NaN值，并返回一个新的DataFrame。该方法是对所有的列进行过滤，那么包含null值和NaN值的行都被过滤。
    *
    * @param df   待处理的DataFrame
    * @param cols 指定需要过滤的列，如果不指定则使用全部列进行过滤
    * @return 一个新的DataFrame
    *
    */
  def filterNullRecord(df: DataFrame, cols: String*) = {
    if (cols.isEmpty) {
      df.na.drop()
    } else {
      df.na.drop(cols)
    }
  }

  /**
    * 使用正则表达式来过滤DataFrame中指定列的记录,返回一个新的DataFrame。
    * 注意该方法对于集合类型和自定义类型的DataType不支持(ArrayType、MapType、StructType)。
    *
    * @param df    需要过滤的DataFrame。
    * @param regex 正则表达式，该正则表达式将应用到指定的所有列上，即每个列都会按照该正则表达式进行过滤.
    * @param cols  指定需要过滤的列名，如果不指定则全部列进行过滤。
    * @return 一个新的DataFrame
    */
  def filterRecordByRegex(df: DataFrame, regex: String, logicType: LogicType.Value, cols: String*) = {
    val sc = df.sqlContext.sparkContext
    val fieldsBuffer = if (cols.isEmpty) df.columns.toSeq else cols
    fieldExistByDataFrame(df, Array(cols: _*), isTrigger = false)
    val bufferDataType = Array(DateType, TimestampType, StringType, FloatType, DoubleType, IntegerType, ShortType, LongType)
    val broadFields = sc.broadcast(fieldsBuffer.filter(field => {
      bufferDataType.contains(df.schema.apply(field).dataType)
    }))
    val broadRegex = sc.broadcast(regex)
    val broadLogicType = sc.broadcast(logicType)
    val rdd = df.rdd.filter(row => {
      val fields = broadFields.value
      if (fields.nonEmpty) {
        val regex = broadRegex.value
        broadLogicType.value match {
          case LogicType.AND => row.getValuesMap[Any](fields).forall(_._2.toString.trim.matches(regex))
          case LogicType.OR => row.getValuesMap[Any](fields).map(_._2.toString.trim.matches(regex)).reduce((b1, b2) => b1 || b2)
          case LogicType.NOT_OR => !row.getValuesMap[Any](fields).map(_._2.toString.trim.matches(regex)).reduce((b1, b2) => b1 || b2)
          case _ => throw new UnsupportedOperationException("不支持的逻辑类型判断!")
        }
      } else {
        false
      }
    })
    val schema = df.schema
    df.sqlContext.createDataFrame(rdd, schema)
  }

  /**
    * 对整型数据进行过滤,保留满足条件整数的记录,整数包括Integer、Long、Int、Short类型.如果未指定列则按照全部列进行过滤;
    * 如果指定了列名则需要所有列满足条件时保留该行记录.仅支持十进制.<br> <br>
    * ALL(全部) :  保留全部的整型数(正数、负数和0),过滤掉不是整型的行记录.<br>
    * NEGATIVE(负数): 保留负整数(所有列满足负整数)，过滤掉正数和0的行记录. <br>
    * POSITIVE(正数): 保留正整数(所有列满足正整数)，过滤掉负数和0的行记录. <br>
    * NONNEGATIVE(非负): 保留正整数和0(所有列满足正整数或0)，过滤掉负数的行记录. <br>
    * NONPOSITIVE(非正): 保留负整数和0(所有列满足负整数或0)，过滤掉正数的行记录. <br>
    *
    * @param df          待进行过滤的DataFrame
    * @param integerType 枚举类型。用于指定需要过滤的整数的类型，包括有 ALL(全部)、NEGATIVE(负数)、POSITIVE(正数)、NONNEGATIVE(非负)和 NONPOSITIVE(非正)
    * @param cols        指定需要过滤的列名，如果不指定则全部列进行过滤。
    * @example
    * <pre>
    * val df= sc.textFile("../../test.txt")
    * +----------+---------+
    * |field_long|field_int|
    * +----------+---------+
    * |-185401   |0        |
    * |133177    |100      |
    * |140065    |71       |
    * |132742    |0        |
    * |130599    |45       |
    * |-109911   |-88      |
    * |110728    |33       |
    * |190376    |-67      |
    * +----------+---------+
    *
    * # case 1
    * ToolUtils.filterNonIntegerRecord(df, NumericType.ALL, Seq("field_long", "field_int", "field_float"): _*)
    * +----------+---------+
    * |field_long|field_int|
    * +----------+---------+
    * +----------+---------+
    *
    * # case 2
    * ToolUtils.filterNonIntegerRecord(df, NumericType.POSITIVE, "field_long", "field_int")
    * +----------+---------+
    * |field_long|field_int|
    * +----------+---------+
    * |133177    |100      |
    * |140065    |71       |
    * |130599    |45       |
    * |110728    |33       |
    * +----------+---------+
    *
    * # case 3
    * ToolUtils.filterNonIntegerRecord(df, NumericType.NEGATIVE, "field_long", "field_int")
    * +----------+---------+
    * |field_long|field_int|
    * +----------+---------+
    * |-109911   |-88      |
    * +----------+---------+
    *
    * # case 4
    * ToolUtils.filterNonIntegerRecord(df, NumericType.NONNEGATIVE, "field_long", "field_int")
    * +----------+---------+
    * |field_long|field_int|
    * +----------+---------+
    * |133177    |100      |
    * |140065    |71       |
    * |132742    |0        |
    * |130599    |45       |
    * |110728    |33       |
    * +----------+---------+
    *
    * # case 5
    * ToolUtils.filterNonIntegerRecord(df, NumericType.NONPOSITIVE, "field_long", "field_int")
    * +----------+---------+
    * |field_long|field_int|
    * +----------+---------+
    * |-185401   |0        |
    * |-109911   |-88      |
    * +----------+---------+
    * </pre>
    * @return 一个新的DataFrame
    */
  def filterNonIntegerRecord(df: sql.DataFrame, integerType: NumericType.Value, cols: String*) =
    filterRecordByRegex(df, integerRegrex(integerType), LogicType.AND, cols: _*)

  /**
    * 对小数类型数据进行过滤,保留满足条件小数的记录,小数类型包括Float和Double类型.如果未指定列则按照全部列进行过滤;
    * 如果指定了列名则需要所有列满足条件时保留该行记录.仅支持十进制.<br> <br>
    * ALL(全部) :  保留全部的小数(正数、负数和0),过滤掉不是小数的行记录.<br>
    * NEGATIVE(负数): 保留负小数(所有列满足负小数)，过滤掉正数和0的行记录. <br>
    * POSITIVE(正数): 保留正小数(所有列满足正小数)，过滤掉负数和0的行记录. <br>
    * NONNEGATIVE(非负): 保留正小数和0(所有列满足正小数或0)，过滤掉负数的行记录. <br>
    * NONPOSITIVE(非正): 保留负小数和0(所有列满足负小数或0)，过滤掉正数的行记录. <br>
    *
    * @example
    * <pre>
    * val df= sc.textFile("../../file.txt")
    * +-----------+------------+
    * |field_float|field_double|
    * +-----------+------------+
    * |       95.0|       140.0|
    * |       74.0|         .23|
    * |      -13.0|      -142.0|
    * |       82.0|        18.0|
    * |      -89.0|         0.0|
    * |       90.0|         0.0|
    * |        7.0|       140.0|
    * +-----------+------------+
    *
    * # case 1
    * ToolUtils.filterNonDecimalRecord(df, NumericType.ALL, Seq("field_float", "field_double"): _*)
    * +-----------+------------+
    * |field_float|field_double|
    * +-----------+------------+
    * |       95.0|       140.0|
    * |      -13.0|      -142.0|
    * |       82.0|        18.0|
    * |      -89.0|         0.0|
    * |       90.0|         0.0|
    * |        7.0|       140.0|
    * +-----------+------------+
    *
    * # case 2
    * ToolUtils.filterNonDecimalRecord(df, NumericType.POSITIVE, Seq("field_float", "field_double"): _*)
    * +-----------+------------+
    * |field_float|field_double|
    * +-----------+------------+
    * |       95.0|       140.0|
    * |       82.0|        18.0|
    * |        7.0|       140.0|
    * +-----------+------------+
    *
    * # case 3
    * ToolUtils.filterNonDecimalRecord(df, NumericType.NEGATIVE, Seq("field_float", "field_double"): _*)
    * +-----------+------------+
    * |field_float|field_double|
    * +-----------+------------+
    * |      -13.0|      -142.0|
    * +-----------+------------+
    *
    * # case 4
    * ToolUtils.filterNonDecimalRecord(df, NumericType.NONNEGATIVE, Seq("field_float", "field_double"): _*)
    * +-----------+------------+
    * |field_float|field_double|
    * +-----------+------------+
    * |       95.0|       140.0|
    * |       82.0|        18.0|
    * |       90.0|         0.0|
    * |        7.0|       140.0|
    * +-----------+------------+
    *
    * # case 5
    * ToolUtils.filterNonDecimalRecord(df, NumericType.NONPOSITIVE, Seq("field_float", "field_double"): _*)
    * +-----------+------------+
    * |field_float|field_double|
    * +-----------+------------+
    * |      -13.0|      -142.0|
    * |      -89.0|         0.0|
    * +-----------+------------+
    *
    * </pre>
    * @param df          待进行过滤的DataFrame
    * @param deciamlType 枚举类型。用于指定需要过滤的小数的条件类型，包括有 ALL(全部)、NEGATIVE(负数)、POSITIVE(正数)、NONNEGATIVE(非负)和 NONPOSITIVE(非正)
    * @param cols        指定需要过滤的列名，如果不指定则全部列进行过滤。
    * @return 一个新的DataFrame
    */
  def filterNonDecimalRecord(df: sql.DataFrame, deciamlType: NumericType.Value, cols: String*) =
    filterRecordByRegex(df, decimalRegrex(deciamlType), LogicType.AND, cols: _*)

  /**
    * 对数值类型数据进行过滤,数值类型数据包括整型数据和小数.暂时不支持复数的过滤,仅支持十进制.<br>
    * <pre>
    * 可支持的格式:
    * 整型数据 -> 10,-10,0
    * 浮点数 -> -12.0, -12.20, -15.023, 14.0, 15., 15.36, 15.0, 19.023,15.0000
    * 指数 -> 0.0E12,12.2e16,-15.0e16 , -17.e16 , -0.23E36 ,  0.28E12 ,  23E36
    * 不支持的格式: .12 , .23e45 , -.12e16 , -012.23
    * </pre>
    *
    * @param df   待进行过滤的DataFrame
    * @param cols 指定需要过滤的列名，如果不指定则全部列进行过滤。
    * @return 一个新的DataFrame
    *
    */
  def filterNonNumericRecord(df: sql.DataFrame, cols: String*) = {
    /**
      * 对于该正则表达式的测试：\n
      * println("100".matches(regex)) // true   \n
      * println("0100".matches(regex)) // false
      * println("-100".matches(regex)) //true
      * println("-0100".matches(regex)) // false
      * println("100.".matches(regex)) // false
      * println("100.2".matches(regex)) // true
      * println("-100.".matches(regex)) //false
      * println("-100.0".matches(regex)) // true
      * println("------------------------------------")
      * println("0".matches(regex)) //true
      * println("-0".matches(regex)) //  true
      * println("-0.0".matches(regex)) // true
      * println("0.0".matches(regex)) //true
      * println("------------------------------------")
      * println("10.0E10".matches(regex)) //true
      * println("-10.0E10".matches(regex)) // true
      * println("10.E10".matches(regex)) //false
      * println("10.-E10".matches(regex)) //false
      * println("10.0E-10".matches(regex)) //true
      * println("------------------------------------")
      * println("-0.0E-10".matches(regex)) // true
      * println("-0.E-10".matches(regex)) // false
      * println("0.0E-10".matches(regex)) //true
      * println("0.E-10".matches(regex)) //false
      * println("-0.1E10".matches(regex)) //true
      * println("-0.0E-10".matches(regex)) //true
      **/
    val regex = "^-?(([1-9]\\d*\\.?\\d*|0\\.[0-9]+\\d*)[Ee]?(-?\\d*)|0)$"
    filterRecordByRegex(df, regex, LogicType.AND, cols: _*)
  }

  /**
    * 对特殊字符进行过滤,如果未指定列名则所有的列都被用于计算,如果指定了列名则任何一列的出现了该特殊字符时该行记录被过滤。
    * 如果当前的特殊字符不能满足需求,可以调用filterRecordByRegex方法来自定义. 默认连接符(-)没有被过滤.
    *
    * @param df    待进行过滤的DataFrame
    * @param isDot 是否将点(.)作为特殊字符处理，False(不作为特殊字符处理),True作为特殊字符处理
    * @param cols  指定需要过滤的列名，如果不指定则全部列进行过滤
    */
  def filterSpecialChar(df: sql.DataFrame, isDot: Boolean, cols: String*) = {
    val regex = if (isDot) "[^(`~!@#$%^&*()_\\+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？)]*"
    else "[^(`~!@#$%^&*()_\\+=|{}':;',\\[\\]<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？)]*"
    filterRecordByRegex(df, regex, LogicType.AND, cols: _*)
  }

  /**
    * 对非中文汉字进行过滤，保留合法的中文汉字。
    *
    * @param df   待进行过滤的DataFrame
    * @param cols 指定需要过滤的列名，如果不指定则全部列进行过滤
    * @return 一个新的DataFrame
    */
  def filterNonCharacter(df: sql.DataFrame, cols: String*) = {
    val regex = "^[\\u4E00-\\u9FFF]+$"
    filterRecordByRegex(df, regex, LogicType.AND, cols: _*)
  }

  /**
    * 对非英文字母进行过滤，保留合法的英文单词。
    *
    * @param df   待进行过滤的DataFrame
    * @param cols 指定需要过滤的列名，如果不指定则全部列进行过滤
    * @return 一个新的DataFrame
    */
  def filterNonLetter(df: sql.DataFrame, cols: String*) = {
    val regex = "^[a-zA-Z]+$"
    filterRecordByRegex(df, regex, LogicType.AND, cols: _*)
  }

  /**
    * 对十进制的IP地址进行过滤，保留合法的IP地址。对于前缀表示法的IP地址同样有效，但未判定网络前缀是否超过了32位。
    *
    * @param df   待过滤的DataFrame
    * @param cols 待过滤的数据列，如果不选择则进行全部列进行过滤
    * @return 一个新的DataFrame
    */
  def filterNonIPAddress(df: sql.DataFrame, cols: String*) = {
    val sc = df.sqlContext.sparkContext
    val selectCols = if (cols.nonEmpty) cols else df.columns.toSeq
    val broadFields = sc.broadcast(selectCols)
    val broadRegex = sc.broadcast("^(?:(?:2[0-4][0-9]\\.)|(?:25[0-5]\\.)" +
      "|(?:1[0-9][0-9]\\.)|(?:[1-9][0-9]\\.)" +
      "|(?:[0-9]\\.)){3}(?:(?:2[0-5][0-5])" +
      "|(?:25[0-5])|(?:1[0-9][0-9])|(?:[1-9][0-9])|(?:[0-9]))$")

    val rddRow = df.rdd.filter(row => {
      val fields = broadFields.value
      val regex = broadRegex.value
      // forall 使用时 对于Empty会返回true
      // 注意使用网络前缀斜线表示法  ,该方法未判定网络前缀是否超过了32位。
      row.getValuesMap[String](fields).forall(kv => {
        val ip = kv._2
        val idx = ip.lastIndexOf("/")
        if (idx != -1) {
          ip.substring(0, idx).matches(regex)
        } else {
          ip.matches(regex)
        }
      })
    })
    df.sqlContext.createDataFrame(rddRow, df.schema)
  }

  /**
    * 判断一个字符串是否是数值类型,数值类型包括整数和小数。该方法仅用于判断是否为数值类型而不进行精度判断。
    *
    * @param `match` 待匹配的字符串
    * @param msg     自定义的消息类型,其模板为: "输入的 `$msg` 值 ${`match`} 不是数值类型！"
    * @param verbos  用于控制匹配失败时是否打印输出异常信息,默认true。
    */
  def isNumeric(`match`: String, msg: String = "", verbos: Boolean = true) = {
    val pattern = "^-?(([1-9]\\d*\\.?\\d*|0\\.[0-9]+\\d*)[Ee]?(-?\\d*)|0)$"
    if (`match`.matches(pattern)) {
      true
    } else {
      if (verbos) throw new IllegalArgumentException(s"输入的 $msg 值 ${`match`} 不是数值类型！")
      false
    }
  }

  /**
    * 判断一个字符串是否是整型类型,整型类型包括Integer、Int、Long、Short、Byte。该方法仅用于判断是否为整型类型而不进行精度判断。
    *
    * @param `match`     待匹配的字符串
    * @param msg         自定义的消息类型,其模板为: "输入的 `$msg` 值 ${`match`} 不是整型类型！"
    * @param integerType 枚举类型。用于指定需要过滤的整数的类型，包括有 ALL(全部)、NEGATIVE(负数)、POSITIVE(正数)、NONNEGATIVE(非负)和 NONPOSITIVE(非正)
    * @param verbos      用于控制匹配失败时是否打印输出异常信息,默认true。
    */
  def isInteger(`match`: String, integerType: NumericType.Value, msg: String = "", verbos: Boolean = true) = {
    val regex = integerRegrex(integerType)
    if (`match`.matches(regex)) {
      true
    } else {
      if (verbos) throw new IllegalArgumentException(s"输入的 $msg 值 ${`match`} 不是整型类型！")
      false
    }
  }

  /**
    * 判断一个字符串是否是小数类型(必须包含小数点),小数类型包括Float和Double。该方法仅用于判断是否为小数类型而不进行精度判断。
    *
    * @param `match`     待匹配的字符串
    * @param msg         自定义的消息类型,其模板为: "输入的 `$msg` 值 ${`match`} 不是浮点数类型！"
    * @param deciamlType 枚举类型。用于指定需要过滤的小数类型，包括有 ALL(全部)、NEGATIVE(负数)、POSITIVE(正数)、NONNEGATIVE(非负)和 NONPOSITIVE(非正)
    * @param verbos      用于控制匹配失败时是否打印输出异常信息,默认true。
    */
  def isDecimal(`match`: String, deciamlType: NumericType.Value, msg: String = "", verbos: Boolean = true) = {
    val regex = decimalRegrex(deciamlType)
    if (`match`.matches(regex)) true
    else {
      if (verbos) throw new IllegalArgumentException(s"输入的值 ${`match`} 不是浮点数类型！")
      false
    }
  }

  val integerRegrex: PartialFunction[NumericType.Value, String] = {
    case NumericType.ALL => "^-?[1-9]\\d*$"
    case NumericType.NEGATIVE => "^-[1-9]\\d*$"
    case NumericType.POSITIVE => "^[1-9]\\d*$"
    case NumericType.NONNEGATIVE => "^[1-9]\\d*|0$"
    case NumericType.NONPOSITIVE => "^-[1-9]\\d*|0$"
  }

  val decimalRegrex: PartialFunction[NumericType.Value, String] = {
    case NumericType.ALL => "^-?(([1-9]\\d*\\.\\d+|0\\.[0-9]+\\d*)[Ee]?(-?\\d*)|0)$"
    case NumericType.POSITIVE => "^(([1-9]\\d*\\.\\d+|0\\.[0-9]+\\[1-9]*)[Ee]?(-?\\d*))$"
    case NumericType.NEGATIVE => "^-(([1-9]\\d*\\.\\d+|0\\.[0-9]+\\[1-9]*)[Ee]?(-?\\d*))$"
    case NumericType.NONPOSITIVE => "^-(([1-9]\\d*\\.\\d+|0\\.[0-9]+\\d*)[Ee]?(-?\\d*)|0)$"
    case NumericType.NONNEGATIVE => "^(([1-9]\\d*\\.\\d+|0\\.[0-9]+\\d*)[Ee]?(-?\\d*)|0)$"
  }

  /**
    * 将String转换为指定的数据类型
    *
    * @param value 指定待转换的字符串
    * @tparam T 指定目标类型
    * @return 根据指定的目标类型，将字符串经进行转换对应的类型并输出
    */
  def toType[T: scala.reflect.runtime.universe.TypeTag](value: String): T = {
    import scala.reflect.runtime.universe._
    val tag = typeTag[T]
    try {
      val v = tag match {
        case TypeTag.Int => value.toInt
        case TypeTag.Byte => value.toByte
        case TypeTag.Short => value.toShort
        case TypeTag.Float => value.toFloat
        case TypeTag.Double => value.toDouble
        case TypeTag.Long => value.toLong
        case TypeTag.Boolean => value.toBoolean
        case _ => throw new UnsupportedOperationException()
      }
      v.asInstanceOf[T]
    } catch {
      case unsupportedEX: UnsupportedOperationException => throw new UnsupportedOperationException(s"暂不支持的数据类型: ${tag.tpe},details:${unsupportedEX.getMessage}")
      case ex: Exception => throw new Exception(s"输入的值 $value 不能转换为类型 ${tag.tpe},details:${ex.getMessage}")
    }
  }

  /**
    * 将String类型转换为指定的类型，可以根据sign参数控制符号(正数、负数或0).<br>
    * ALL: 不进行符号的判断   <br>
    * NEGATIVE: 判定是否为负数,否则产生异常    <br>
    * POSITIVE: 判定是否为正数,否则产生异常      <br>
    * NONNEGATIVE: 判定是否为正数和0,否则产生异常    <br>
    * NONPOSITIVE:  判定是否为负数和0,否则产生异常    <br>
    *
    * @param value   待类型转换的字符串
    * @param msgInfo 自定义异常信息提示信息
    * @param sign    指定转换后值符号:包括有 ALL(不进行符号判断)、NEGATIVE(负数)、POSITIVE(正数)、NONNEGATIVE(非负)和 NONPOSITIVE(非正)
    * @tparam T 目标数据类型
    * @return
    */
  def toSignType[T: scala.reflect.runtime.universe.TypeTag](value: String, msgInfo: String = "")(sign: NumericType.Value): T = {
    import scala.reflect.runtime.universe._
    var msg = msgInfo
    if (null == msg || msg == "")
      msg = s"确保输入的值 $value 在类型转换后能够满足设定的符号!"
    val tag = typeTag[T]
    try {
      val v = tag match {
        case TypeTag.Int => computeNumericType[Int](value.toInt, 0, sign, msg)
        case TypeTag.Byte => computeNumericType[Byte](value.toByte, 0, sign, msg)
        case TypeTag.Short => computeNumericType[Short](value.toShort, 0, sign, msg)
        case TypeTag.Float => computeNumericType[Float](value.toFloat, 0, sign, msg)
        case TypeTag.Double => computeNumericType[Double](value.toDouble, 0, sign, msg)
        case TypeTag.Long => computeNumericType[Long](value.toLong, 0, sign, msg)
        case _ => throw new UnsupportedOperationException()
      }
      v.asInstanceOf[T]
    } catch {
      case unsupportedEX: UnsupportedOperationException => throw new UnsupportedOperationException(s"暂不支持的数据类型: ${tag.tpe},details:${unsupportedEX.getMessage}")
      case ex: Exception => throw new Exception(s"输入的值 $value 不能转换为类型 ${tag.tpe},details:${ex.getMessage}")
    }
  }

  private def computeNumericType[T](dataType: T, threshold: T, sign: NumericType.Value, msg: String = "")(implicit ordering: Ordering[T]) = {
    import Ordered._
    sign match {
      case NumericType.POSITIVE => dataType.ensuring(dataType > threshold, msg)
      case NumericType.NONPOSITIVE => dataType.ensuring(dataType <= threshold, msg)
      case NumericType.NEGATIVE => dataType.ensuring(dataType < threshold, msg)
      case NumericType.NONNEGATIVE => dataType.ensuring(dataType >= threshold, msg)
      case NumericType.ALL => dataType.ensuring(true)
    }
  }

  /**
    * 将String类型转换为指定的类型，可以指定范围是否合法!
    *
    * @param value              待类型转换的字符串
    * @param min                指定的最小值
    * @param max                指定的最大值
    * @param leftExOrInClusive  指定在进行范围判断时是否包含最小值,默认值false(不包含).
    * @param rightExOrInClusive 指定在进行范围判断时是否包含最大值,默认值false(不包含).
    * @param msgInfo            自定义异常提示信息
    * @tparam T 目标数据类型
    * @return
    */
  def toRangeType[T: scala.reflect.runtime.universe.TypeTag](value: String, min: T, max: T, leftExOrInClusive: Boolean = false, rightExOrInClusive: Boolean = false, msgInfo: String = ""): T = {
    import scala.reflect.runtime.universe._
    var msg = msgInfo
    if (null == msg || msg == "")
      msg = s"输入的值 $value 在进行转换后的范围不在($min,$max)内!"
    val tag = typeTag[T]
    try {
      val v = tag match {
        case TypeTag.Int => computeRangeType[Int](value.toInt, min.asInstanceOf[Int], max.asInstanceOf[Int], leftExOrInClusive, rightExOrInClusive, msg)
        case TypeTag.Byte => computeRangeType[Byte](value.toByte, min.asInstanceOf[Byte], max.asInstanceOf[Byte], leftExOrInClusive, rightExOrInClusive, msg)
        case TypeTag.Short => computeRangeType[Short](value.toShort, min.asInstanceOf[Short], max.asInstanceOf[Short], leftExOrInClusive, rightExOrInClusive, msg)
        case TypeTag.Float => computeRangeType[Float](value.toFloat, min.asInstanceOf[Float], max.asInstanceOf[Float], leftExOrInClusive, rightExOrInClusive, msg)
        case TypeTag.Double => computeRangeType[Double](value.toDouble, min.asInstanceOf[Double], max.asInstanceOf[Double], leftExOrInClusive, rightExOrInClusive, msg)
        case TypeTag.Long => computeRangeType[Long](value.toLong, min.asInstanceOf[Long], max.asInstanceOf[Long], leftExOrInClusive, rightExOrInClusive, msg)
        case _ => throw new UnsupportedOperationException()
      }
      v.asInstanceOf[T]
    } catch {
      case unsupportedEX: UnsupportedOperationException => throw new UnsupportedOperationException(s"暂不支持的数据类型: ${tag.tpe},details:${unsupportedEX.getMessage}")
      case illegal: IllegalArgumentException => throw new IllegalArgumentException(s"参数不合法或输入的值 $value 不能转换为类型 ${tag.tpe},details:${illegal.getMessage}")
      case minMaxIllegal: MinMaxIllegalArgumentExcetion => throw new MinMaxIllegalArgumentExcetion(minMaxIllegal.getMessage)
      case valueRange: ValueRangeExcetion => throw new ValueRangeExcetion(valueRange.getMessage)
      case ex: Exception => throw new Exception(s"输入的值 $value 不能转换为类型 ${tag.tpe},details:${ex.getMessage}")
    }


  }

  private def computeRangeType[T](dataType: T, min: T, max: T, leftExOrInClusive: Boolean, rightExOrInClusive: Boolean, msg: String = "")(implicit ordering: Ordering[T]): T = {
    if (ordering.gt(min, max)) throw new MinMaxIllegalArgumentExcetion(s"参数输入不合法，最小值$min 大于了 最大值$max")
    val compareMin = ordering.compare(dataType, min) // must be positive
    val compareMax = ordering.compare(dataType, max) // must be negative
    // if clusive is true , both  leftExOrInClusive   and rightExOrInClusive are equal
    // if clusive is false , both  leftExOrInClusive   and rightExOrInClusive are not equal
    val clusive = leftExOrInClusive ^ rightExOrInClusive
    val bothTrueOrFalse = ((!clusive) && leftExOrInClusive && compareMin >= 0 && compareMax <= 0) ||
      (!clusive && !leftExOrInClusive && compareMin > 0 && compareMax < 0) ||
      (clusive && leftExOrInClusive && !rightExOrInClusive && compareMin >= 0 && compareMax < 0) ||
      (clusive && !leftExOrInClusive && rightExOrInClusive && compareMin > 0 && compareMax <= 0)
    if (!bothTrueOrFalse) throw new ValueRangeExcetion(msg)
    dataType
  }

}

class Toolkit extends Serializable {}

object NumericType extends Enumeration with Serializable {

  /**
    * 证书、负数和零
    */
  val ALL = Value("All")
  /**
    * 正数
    */
  val POSITIVE = Value("Positive")
  /**
    * 负数和零
    */
  val NONPOSITIVE = Value("NonPositive")
  /**
    * 负数
    */
  val NEGATIVE = Value("Negative")
  /**
    * 正数和零
    */
  val NONNEGATIVE = Value("NonNegative")
}

object LogicType extends Enumeration {
  /**
    * 当且仅当所有的列都符合正则表达式时返回true
    */
  val AND = Value(0, "AND")

  /**
    * 当且仅当至少有一列符合正则表达式时返回true
    */
  val OR = Value(1, "OR")

  /**
    * 当且仅当至少有一列符合正则表达式时返回false
    */
  val NOT_OR = Value(2, "NOT_OR")

}


//自定义各种异常
/**
  * String message, Throwable cause,
  * boolean enableSuppression,
  * boolean writableStackTrace
  */
final class ValueRangeExcetion private(msg: String, causeMsg: Throwable, isEnableSuppression: Boolean, isWritableStackTrace: Boolean)
  extends RuntimeException(msg, causeMsg, isEnableSuppression, isWritableStackTrace) {
  private final val serialVersionUID = -2365140125412365412L

  /**
    * Constructs an <code>IllegalArgumentException</code> with no
    * detail message.
    */
  def this(msg: String) {
    this(msg, null, true, true)
  }

  def this(msg: String, causeMsg: Throwable) {
    this(msg, causeMsg, true, true)
  }

  def this(causeMsg: Throwable) {
    this(if (causeMsg == null) null else causeMsg.toString, causeMsg, true, true)
  }

  def this() {
    this("")
  }
}

final class MinMaxIllegalArgumentExcetion private(msg: String, causeMsg: Throwable, isEnableSuppression: Boolean, isWritableStackTrace: Boolean)
  extends RuntimeException(msg, causeMsg, isEnableSuppression, isWritableStackTrace) {
  private final val serialVersionUID = -2365140125412365412L

  /**
    * Constructs an <code>IllegalArgumentException</code> with no
    * detail message.
    */
  def this(msg: String) {
    this(msg, null, true, true)
  }

  def this(msg: String, causeMsg: Throwable) {
    this(msg, causeMsg, true, true)
  }

  def this(causeMsg: Throwable) {
    this(if (causeMsg == null) null else causeMsg.toString, causeMsg, true, true)
  }

  def this() {
    this("")
  }
}
