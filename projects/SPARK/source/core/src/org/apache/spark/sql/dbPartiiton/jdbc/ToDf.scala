package org.apache.spark.sql.dbPartiiton.jdbc

import java.sql.{Connection, ResultSet, ResultSetMetaData, SQLException}
import org.apache.spark.sql.dbPartiiton.service.ResConx
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.dbPartiiton.service.ConnectionService
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.dbPartiiton.jdbc.AssistFunction._

class ToDf(sc:SparkContext,sqlc:SQLContext) extends Serializable{


  def getCatalystType(
                       sqlType: Int,
                       precision: Int,
                       scale: Int,
                       signed: Boolean): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY         => null
      case java.sql.Types.BIGINT        => if (signed) { LongType } else { DecimalType(20,0) }
      case java.sql.Types.BINARY        => BinaryType
      case java.sql.Types.BIT           => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB          => BinaryType
      case java.sql.Types.BOOLEAN       => BooleanType
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.CLOB          => StringType
      case java.sql.Types.DATALINK      => null
      case java.sql.Types.DATE          => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType.bounded(precision, scale)

      case java.sql.Types.DECIMAL       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT      => null
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.FLOAT         => FloatType
      case java.sql.Types.INTEGER       => if (signed) { IntegerType } else { LongType }
      case java.sql.Types.JAVA_OBJECT   => null
      case java.sql.Types.LONGNVARCHAR  => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR   => StringType
      case java.sql.Types.NCHAR         => StringType
      case java.sql.Types.NCLOB         => StringType
      case java.sql.Types.NULL          => null

      case java.sql.Types.NUMERIC
        if precision == 0  => DecimalType.bounded(38, 0)

      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType.bounded(precision, scale)
      case java.sql.Types.NUMERIC       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR      => StringType
      case java.sql.Types.OTHER         => null
      case java.sql.Types.REAL          => DoubleType
      case java.sql.Types.REF           => StringType
      case java.sql.Types.ROWID         => LongType
      case java.sql.Types.SMALLINT      => IntegerType
      case java.sql.Types.SQLXML        => StringType
      case java.sql.Types.STRUCT        => StringType
      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.TINYINT       => IntegerType
      case java.sql.Types.VARBINARY     => BinaryType
      case java.sql.Types.VARCHAR       => StringType
      case _                            => null
    }
    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }

  def  Struct(url:String,conn:Connection,sql:String) :StructType= {
    val stm = conn.createStatement()
    var sql1 = ""
    var rs: ResultSet = null
    val dialect = JdbcDialects.get(url)
    if (sql.contains("where")) {
      sql1 = sql.replaceAll("where.+", "where 1=0")
    }
    else if (sql.contains("limit")) {
      sql1 = sql.replaceAll("limit.+", "where 1=0")
    }
    else {
      sql1 = sql + " where 1=0"
    }


    rs = stm.executeQuery(sql1)
    val rsmd = rs.getMetaData
    val ncols = rsmd.getColumnCount
    val fields = new Array[StructField](ncols)
    var i = 0
    while (i < ncols) {
      val columnName = rsmd.getColumnLabel(i + 1)
      val dataType = rsmd.getColumnType(i + 1)
      val typeName = rsmd.getColumnTypeName(i + 1)
      val fieldSize = rsmd.getPrecision(i + 1)
      val fieldScale = rsmd.getScale(i + 1)
      val isSigned = rsmd.isSigned(i + 1)
      val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
      val metadata = new MetadataBuilder().putString("name", columnName)
      metadata.putLong("scale",fieldScale)
      val columnType =
        dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
          getCatalystType(dataType, fieldSize, fieldScale, isSigned))
      fields(i) = StructField(columnName, columnType, nullable, metadata.build())
      i = i + 1
    }
    rs.close
    stm.close
    conn.close
    return new StructType(fields)
  }


  def jdbcLong(url:String,driver: String,user:String,passw:String,sql:String,partition:Array[Long],fetchSize:Int,threadCount:Int):DataFrame={


      val service=new ConnectionService
      val conn2= service.getConnection(driver,url, user, passw)
      val fields=Struct(url,conn2,sql)
      val schema = StructType(fields)


      val rdd=new LongRDD[InternalRow](sc,
        () => {

          type Closeable = {
            def close:Unit;
          }
          val resConx =  ResConx.getInstance().conx(url)
          resConx(threadCount).init{
            val con = new ConnectionService().getConnection(driver,url,user,passw)
            con.asInstanceOf[Closeable]
          }

          resConx.getRes.asInstanceOf[java.sql.Connection]

        },
        sql, partition,fetchSize,

        rs => resultSet2Row(rs, schema)
      )
      val outDf = sqlc.internalCreateDataFrame(rdd, schema)
      outDf
    }

  //需要有field字段，partition数组的内容为纯边界，没有字段。
  def jdbcString(url:String,driver: String,user:String,passw:String,field:String,sql:String,partition:Array[String],fetchSize:Int,threadCount:Int):DataFrame={

    val service=new ConnectionService
    val conn2= service.getConnection(driver,url, user, passw)
    val fields=Struct(url,conn2,sql)
    val schema = StructType(fields)

    val rdd=new StringRDD[InternalRow](sc,
      () => {

        type Closeable = {
          def close:Unit;
        }
        val resConx =  ResConx.getInstance().conx(url)
        resConx(threadCount).init{
          val con = new ConnectionService().getConnection(driver,url,user,passw)
          con.asInstanceOf[Closeable]
        }


        resConx.getRes.asInstanceOf[java.sql.Connection]

      },
      sql,field,partition, fetchSize,

      rs => resultSet2Row(rs, schema)
    )
    val outDf = sqlc.internalCreateDataFrame(rdd, schema)
      outDf
  }

  def   jdbc(url:String,driver: String,user:String,passw:String,sql:String,partition:Array[String],fetchSize:Int,threadCount:Int):DataFrame={

    val service=new ConnectionService
    val conn2= service.getConnection(driver,url, user, passw)
    val fields=Struct(url,conn2,sql)
    val schema = StructType(fields)

    val rdd=new JdbcRDD[InternalRow](sc,
      () => {

        type Closeable = {
          def close:Unit;
        }
        val resConx =  ResConx.getInstance().conx(url)
        resConx(threadCount).init{
          val con = new ConnectionService().getConnection(driver,url,user,passw)
          con.asInstanceOf[Closeable]
        }

        new org.apache.log4j.helpers.SyslogQuietWriter(new org.apache.log4j.helpers.SyslogWriter("192.168.15.25:514"), 600,
          new org.apache.log4j.varia.FallbackErrorHandler()).write("pool size:" + resConx.count())

        resConx.getRes.asInstanceOf[java.sql.Connection]
      },
      sql,partition, fetchSize,
      rs => resultSet2Row(rs, schema)
    )

    val outDf = sqlc.internalCreateDataFrame(rdd, schema)
    outDf
  }

  def  rowNumJdbc(url:String,driver:String,user:String,passw:String,dbType:String,sql:String,down:Long,up:Long,partitionsNum:Int,fetchSize:Int,threadCount:Int):DataFrame={

    val service=new ConnectionService
    val conn2= service.getConnection(driver,url, user, passw)
    val fields=Struct(url,conn2,sql)
    val schema = StructType(fields)

    val rdd=new RowNumRDD[InternalRow](sc,
      () => {

        type Closeable = {
          def close:Unit
        }
        val resConx =  ResConx.getInstance().conx(url)
        resConx(threadCount).init{
          val con = new ConnectionService().getConnection(driver,url,user,passw)
          con.asInstanceOf[Closeable]
        }

        new org.apache.log4j.helpers.SyslogQuietWriter(new org.apache.log4j.helpers.SyslogWriter("192.168.15.25:514"), 600,
          new org.apache.log4j.varia.FallbackErrorHandler()).write("pool size:" + resConx.count())

        resConx.getRes.asInstanceOf[java.sql.Connection]

      },
      dbType,sql,down,up,partitionsNum,fetchSize,
      rs => resultSet2Row(rs, schema)
    )
    val outDf = sqlc.internalCreateDataFrame(rdd, schema)
    outDf

  }

}

