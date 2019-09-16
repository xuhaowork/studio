package org.apache.spark.sql.dbPartiiton.dataType

class MongoDataType  extends  TypeConver{

  override def conversion(str: String): String = {
    str.toLowerCase  match {
         case  "date" =>  "Date"
         case  "double"=> "Number"
         case  "integer" =>"Number"
         case  "min key"=>"Number"
         case  "max key"=>"Number"
         case  "timestamp" =>"Number"
         case   _=>"String"
      }
  }
}




