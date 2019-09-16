package org.apache.spark.sql.dbPartiiton.dataType

class PostgreDataType extends TypeConver {

  override def conversion(str: String): String = {

       str.toLowerCase match{
         case  "int2"=> "Long"
         case  "int4"=> "Long"
         case  "int8"=> "Long"
         case  "serial2"=>"Long"
         case  "serial4"=>"Long"
         case  "serial8"=>"Long"
         case  "float4" =>"Double"
         case  "float8"=>"Double"
         case  "decimal"=>"Double"
         case  "date"=>"Timestamp"
         case  "time"=>"Timestamp"
         case  "timetz"=>"Timestamp"
         case  "timestamp"=>"Timestamp"
         case  "timestamptz"=>"Timestamp"
         case  _=>"String"
        }
  }
}
