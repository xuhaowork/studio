package org.apache.spark.sql.dbPartiiton.dataType

class OraDataType  extends  TypeConver{

  override def conversion(str:String): String ={

     str.toUpperCase() match{
       case   "LONG"     => "Long"
       case   "INTEGER" => "Long"
       case   "INT"     => "Long"
       case   "SMALLINT"  => "Long"
       case   "NUMBER"   =>  "Double"
       case   "BINARY_FLOAT" => "Double"
       case   "BINARY_DOUBLE"  => "Double"
       case   "NUMERIC"    => "Double"
       case   "DECIMAL"    => "Double"
       case   "FLOAT"      => "Double"
       case   "DOUBLE"     => "Double"
       case   "PRECISION"   => "Double"
       case   "DATE"        => "Timestamp"
       case   "INTERVAL DAY TO SECOND"        => "Timestamp"
       case   "INTERVAL YEAR TO MONTH"        => "Timestamp"
       case   "TIMESTAMP"        => "Timestamp"
       case   "TIMESTAMP WITH TIME ZONE"        => "Timestamp"
       case   "TIMESTAMP WITH LOCAL TIME ZONE"        => "Timestamp"
       case    _ => "String"

      }

  }

}
