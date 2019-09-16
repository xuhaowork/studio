package org.apache.spark.sql.writer

import java.sql.{Connection, PreparedStatement}


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.dbPartiiton.service.{ConnectionService, ResConx}

object SchemaWriter  {

  def oracleSchema(df: DataFrame, url: String): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    df.schema.fields foreach { field => {
      val name = field.name
      val typ: String =
        dialect.getJDBCType(field.dataType).map(_.databaseTypeDefinition).getOrElse(
          field.dataType match {
            case IntegerType => "INTEGER"
            case LongType => "LONG"
            case DoubleType => "DOUBLE PRECISION"
            case FloatType => "REAL"
            case ShortType => "INTEGER"
            case ByteType => "BYTE"
            case BooleanType => "BIT(1)"
            case StringType => "CLOB"
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


  def createTable(df:DataFrame,DBType:String,driver:String,url:String,user:String,password:String,table:String):Unit={

     val conn: Connection = new  ConnectionService().getConnection(driver,url,user,password)
     var schema=""
     try {
       if(DBType=="Oracle"){
         schema =oracleSchema(df, url)
       } else{
         schema = JdbcUtils.schemaString(df, url)
       }
       val sql = s"CREATE TABLE $table ($schema)"
       conn.prepareStatement(sql).executeUpdate()
       println("建表成功")
       } finally {
         conn.close()
       }
     }





}