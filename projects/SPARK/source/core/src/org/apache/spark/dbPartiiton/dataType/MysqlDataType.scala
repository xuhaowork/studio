package org.apache.spark.sql.dbPartiiton.dataType

class MysqlDataType  extends  TypeConver{

  override def conversion(str:String): String ={

      str.toLowerCase match {
        case     "tinyint" =>  "Long"
        case     "smallint"=>  "Long"
        case     "mediumint"=> "Long"
        case     "int"=>       "Long"
        case     "integer" =>  "Long"
        case     "bigint" =>   "Long"
        case     "real" =>     "Double"
        case     "double"=>    "Double"
        case     "float" =>    "Double"
        case     "decimal" =>  "Double"
        case     "numeric"=>   "Double"
        case     "date" =>  "Timestamp"
        case     "time" =>   "Timestamp"
        case     "year" =>  "Timestamp"
        case     "timestamp" =>"Timestamp"
        case     "datetime" => "Timestamp"
        case     _ =>"String"
      }

  }

}
