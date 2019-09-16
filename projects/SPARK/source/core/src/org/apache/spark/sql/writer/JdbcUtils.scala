package org.apache.spark.sql.writer

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.internal.Logging
//import java.util.Properties

import scala.util.Try
//import org.apache.spark.Logging
import org.apache.spark.sql.dbPartiiton.service.{ConnectionService, ResConx}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
//import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD


object JdbcUtils extends  Logging {

  /*def createConnection(url: String, connectionProperties: Properties): Connection = {
    JDBCRDD.getConnector(connectionProperties.getProperty("driver"), url, connectionProperties)()
  }*/

  def tableExists(conn: Connection, table: String): Boolean = {

    Try(conn.prepareStatement(s"SELECT 1 FROM $table LIMIT 1").executeQuery().next()).isSuccess
  }

  def dropTable(conn: Connection, table: String): Unit = {
    conn.prepareStatement(s"DROP TABLE $table").executeUpdate()
  }


  def insertStatement(conn: Connection, table: String, rddSchema: StructType): PreparedStatement = {
    val sql = new StringBuilder(s"INSERT INTO $table VALUES (")
    var fieldsLeft = rddSchema.fields.length
    while (fieldsLeft > 0) {
      sql.append("?")
      if (fieldsLeft > 1) sql.append(", ") else sql.append(")")
      fieldsLeft = fieldsLeft - 1
    }
    conn.prepareStatement(sql.toString())
  }

  def savePartition(
                     getConnection: () => Connection,
                     table: String,
                     iterator: Iterator[Row],
                     rddSchema: StructType,
                     nullTypes: Array[Int]): Iterator[Byte] = {
    val conn = getConnection()
/**
  *
  * 事务提交还得做，回滚是有必要的。
  *
* */
    val url=conn.getMetaData.getURL
   // var committed = false
    try {
     // conn.setAutoCommit(false)
      val stmt = insertStatement(conn, table, rddSchema)
      try {
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = rddSchema.fields.length
          var i = 0
          //改动的地方
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              rddSchema.fields(i).dataType match {
                case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                case LongType => stmt.setLong(i + 1, row.getLong(i))
                case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                case ShortType => stmt.setInt(i + 1, row.getShort(i))
                case ByteType => stmt.setInt(i + 1, row.getByte(i))
                case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                case StringType => stmt.setString(i + 1, row.getString(i))
                case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
                case _ => throw new IllegalArgumentException(
                  s"Can't translate non-null value for field $i")
              }
            }
            i = i + 1
          }
          stmt.executeUpdate()
        }
      }
      /* finally {
        stmt.close()
      }*/

     /* conn.commit()
      committed = true*/

      //Array[Byte]().iterator

    }  finally {
     /* if (!committed) {
        conn.rollback()
        conn.close()
       /* type Closeable = {
          def close:Unit;
        }
        ResConx.getInstance().conx().retRes(conn.asInstanceOf[Closeable])

        new org.apache.log4j.helpers.SyslogQuietWriter(new org.apache.log4j.helpers.SyslogWriter("192.168.15.25:514"), 600,
          new org.apache.log4j.varia.FallbackErrorHandler()).write("after ret pool size:" + ResConx.getInstance().conx() + ":" +  ResConx.getInstance().conx().count())
       */
      } else {*/
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
         //conn.close()
         type Closeable = {
            def close:Unit;
          }
          ResConx.getInstance().conx(url).retRes(conn.asInstanceOf[Closeable])

        } catch {
          case e: Exception => logWarning("Transaction succeeded, but connection releasing  failed", e)
        }
      }

    //}
    Array[Byte]().iterator

  }


  def schemaString(df: DataFrame, url: String): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    df.schema.fields foreach { field => {
      val name = field.name
      val typ: String =
        dialect.getJDBCType(field.dataType).map(_.databaseTypeDefinition).getOrElse(
          field.dataType match {
            case IntegerType => "INTEGER"
            case LongType => "BIGINT"
            case DoubleType => "DOUBLE PRECISION"
            case FloatType => "REAL"
            case ShortType => "INTEGER"
            case ByteType => "BYTE"
              // GBase8t 没有这种数据类型，而且GBase8t的值为"t","f", oracle没有bit  用char(1)替代。
            case BooleanType => "BIT(1)"
            case StringType => "TEXT"
            case BinaryType => "BLOB"
            case TimestampType => "TIMESTAMP"
            case DateType => "DATE"
            case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
            case _ => throw new IllegalArgumentException(s"Don't know how to save $field to JDBC")
          })
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }}
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
    * Saves the RDD to the database in a single transaction.
    */
  def saveTable(
                 df: DataFrame,
                 url: String,
                 table: String,
                 driver:String,
                 user:String,
                 passw:String,
                 threadCount: Int) {
    val dialect = JdbcDialects.get(url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      dialect.getJDBCType(field.dataType).map(_.jdbcNullType).getOrElse(
        field.dataType match {
          case IntegerType => java.sql.Types.INTEGER
          case LongType => java.sql.Types.BIGINT
          case DoubleType => java.sql.Types.DOUBLE
          case FloatType => java.sql.Types.REAL
          case ShortType => java.sql.Types.INTEGER
          case ByteType => java.sql.Types.INTEGER
          case BooleanType => java.sql.Types.BIT
          case StringType => java.sql.Types.CLOB
          case BinaryType => java.sql.Types.BLOB
          case TimestampType => java.sql.Types.TIMESTAMP
          case DateType => java.sql.Types.DATE
          case t: DecimalType => java.sql.Types.DECIMAL
          case _ => throw new IllegalArgumentException(
            s"Can't translate null value for field $field")
        })
    }

    val rddSchema = df.schema

   // val getConnection: () => Connection = JDBCRDD.getConnector(driver, url, properties)
    df.foreachPartition { iterator =>
      savePartition( () => {

       // new ConnectionService().getConnection(driver,url,user,passw)

        type Closeable = {
          def close:Unit
        }
        val resConx =  ResConx.getInstance().conx(url)
        resConx(threadCount).init{
          val con = new ConnectionService().getConnection(driver,url,user,passw)
          //val con:Connection=null
          con.asInstanceOf[Closeable]
        }


        resConx.getRes.asInstanceOf[java.sql.Connection]

      }, table, iterator, rddSchema, nullTypes)
    }
  }

}

