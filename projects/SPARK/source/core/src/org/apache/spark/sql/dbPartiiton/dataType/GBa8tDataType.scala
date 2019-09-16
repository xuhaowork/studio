package org.apache.spark.sql.dbPartiiton.dataType

class GBa8tDataType  extends  TypeConver{
  override def conversion(str: String): String = {

    str.toLowerCase match{
       case "bigserial" => "Long"
       case "bigint"=>"Long"
       case "int"=>"Long"
       case "int8"=>"Long"
       case "serial"=>"Long"
       case "serial8"=>"Long"
       case "smallint"=>"Long"
       case "decimal"=>"Double"
       case "float"=>"Double"
       case "date"=>"Timestamp"
       case "datetime"=>"Timestamp"
       case "year to day"=>"Timestamp"
       case "datetime year to second" =>"Timestamp"
       case "datetime year to fraction(3)"=>"Timestamp"
       case  _=>"String"
     }
  }

}
