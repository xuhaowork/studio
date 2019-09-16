package org.apache.spark.sql.dbPartiiton.jdbc

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.unsafe.types.UTF8String
import java.sql.ResultSet

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow

object AssistFunction extends Serializable {

  abstract class JDBCConversion
  case object BooleanConversion extends JDBCConversion
  case object DateConversion extends JDBCConversion
  case class DecimalConversion(precision: Int, scale: Int) extends JDBCConversion
  case object DoubleConversion extends JDBCConversion
  case object FloatConversion extends JDBCConversion
  case object IntegerConversion extends JDBCConversion
  case object LongConversion extends JDBCConversion
  case object BinaryLongConversion extends JDBCConversion
  case object StringConversion extends JDBCConversion
  case object TimestampConversion extends JDBCConversion
  case object BinaryConversion extends JDBCConversion

  def getConversions(schema: StructType): Array[JDBCConversion] = {
    schema.fields.map(sf => {
      sf.dataType match {
        case BooleanType => BooleanConversion
        case DateType => DateConversion
        case DecimalType.Fixed(p, s) => DecimalConversion(p, s)
        case DoubleType => DoubleConversion
        case FloatType => FloatConversion
        case IntegerType => IntegerConversion
        case LongType =>
          if (sf.metadata.contains("binarylong")) BinaryLongConversion else LongConversion
        case StringType => StringConversion
        case TimestampType => TimestampConversion
        case BinaryType => BinaryConversion
        case _ => throw new IllegalArgumentException(s"Unsupported field $sf")
      }
    }).toArray
  }

  def resultSet2Row(rs: ResultSet, schema: StructType): InternalRow = {
    var i = 0
    val conversions = getConversions(schema)
    val mutableRow = new SpecificMutableRow(schema.fields.map(x => x.dataType))
    while (i < conversions.length) {
      val pos = i + 1
      conversions(i) match {
        case BooleanConversion => mutableRow.setBoolean(i, rs.getBoolean(pos))
        case DateConversion =>
          // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
          val dateVal = rs.getDate(pos)
          if (dateVal != null) {
            mutableRow.setInt(i, DateTimeUtils.fromJavaDate(dateVal))
          } else {
            mutableRow.update(i, null)
          }
        case DecimalConversion(p, s) =>
          val decimalVal = rs.getBigDecimal(pos)
          if (decimalVal == null) {
            mutableRow.update(i, null)
          } else {
            mutableRow.update(i, Decimal(decimalVal, p, s))
          }
        case DoubleConversion => mutableRow.setDouble(i, rs.getDouble(pos))
        case FloatConversion => mutableRow.setFloat(i, rs.getFloat(pos))
        case IntegerConversion => mutableRow.setInt(i, rs.getInt(pos))
        case LongConversion => mutableRow.setLong(i, rs.getLong(pos))
        // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
        case StringConversion => mutableRow.update(i, UTF8String.fromString(rs.getString(pos)))
        case TimestampConversion =>
          val t = rs.getTimestamp(pos)
          if (t != null) {
            mutableRow.setLong(i, DateTimeUtils.fromJavaTimestamp(t))
          } else {
            mutableRow.update(i, null)
          }
        case BinaryConversion => mutableRow.update(i, rs.getBytes(pos))
        case BinaryLongConversion => {
          val bytes = rs.getBytes(pos)
          var ans = 0L
          var j = 0
          while (j < bytes.size) {
            ans = 256 * ans + (255 & bytes(j))
            j = j + 1
          }
          mutableRow.setLong(i, ans)
        }
      }
      if (rs.wasNull) mutableRow.setNullAt(i)
      i = i + 1
    }
    mutableRow
  }
}
