package org.apache.spark.sql.dbPartiiton.dataType

object TypeConverFactory {


    def  oracleConver(str:String):String={
       new  OraDataType().conversion(str)
    }


    def  mysqlConver(str:String):String={
      new   MysqlDataType().conversion(str)
    }


    def  gbase8tConver(str:String):String={
       new  GBa8tDataType().conversion(str)
    }


    def  postgresqlConver(str:String):String={
       new  PostgreDataType().conversion(str)
    }

    def  sqlserverConver(str:String):String={
      new  SqlDataType().conversion(str)
    }

    def conver(str1:String,str2:String):String={

      str1 match {
        case   "Oracle"=>oracleConver(str2)
        case   "SqlServer"=>sqlserverConver(str2)
        case   "Gbase8t"=> gbase8tConver(str2)
        case   "Postgre" | "MPPDB" =>postgresqlConver(str2)
        case   "MySQL" | "Gbase8a"=>mysqlConver(str2)

      }

    }


}
