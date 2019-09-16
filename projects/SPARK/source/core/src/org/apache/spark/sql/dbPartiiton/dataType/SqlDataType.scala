package org.apache.spark.sql.dbPartiiton.dataType

class SqlDataType  extends   TypeConver{

  override   def  conversion(str:String):String={

     str.toLowerCase  match  {
       case    "bigint" => "Long"
       case    "int" =>  "Long"
       case    "smallint" => "Long"
       case    "tinyint" => "Long"
       case    "decimal"=> "Double"
       case    "float"=>"Double"
       case    "numeric"=>"Double"
       case    "real"=>"Double"
       case    "date"=>"Timestamp"
       case    "datetime"=>"Timestamp"
       case    "datetime2" =>"Timestamp"
       case    "datetimeoffset" =>"Timestamp"
       case    "time"=>"Timestamp"
       case    "timestamp"=>"Timestamp"
       case     _=> "String"
     }

  }

}
