package org.apache.spark.sql.dbPartiiton.partition

import com.google.gson.JsonArray
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}
import  org.apache.spark.sql.dbPartiiton.dataType.MongoDataType
object Analysis {

  def caseUnify(sql:String):String={
   sql.replaceAll("[Ss][Ee][Ll][Ee][Cc][Tt]", "select" ).replaceAll("[Ww][Hh][Ee][Rr][Ee]", "where" ).replaceAll("[Aa][Nn][Dd]", "and").replaceAll("[Ff][Rr][Oo][Mm]","from").replaceAll("[Ll][Ii][Mm][Ii][Tt]","limit")
  }

 def  sqlExract(sql:String,column:String):(String,String,String)={
   val str=caseUnify(sql)
   if (str.contains("where")) {
     val strTem = str.split("where")
     val strTem1 = strTem(0)
     val strTem2 = strTem(1).split("and").toBuffer

     var down = ""
     var up = ""

     for (k <- 0 until strTem2.length) {
       if (strTem2(k).contains(column)) {
         if (strTem2(k).contains(">")) {
           down = strTem2(k)
         }
         if (strTem2(k).contains("<")) {
           up = strTem2(k)
         }
       }
     }

     val strTem3 = strTem2.-(down, up)
     var strTem4 = ""

     for (i <- 0 until strTem3.length) {
       if (i == 0) {
         strTem4 = strTem3(0).replaceAll(" ", "")
       } else {
         strTem4 = strTem4 + " and " + strTem3(i).replaceAll(" ", "")
       }
     }

     down = down.replaceAll(" ", "")
     up = up.replaceAll(" ", "")

     var sqlFliter = ""
     if (strTem4.length != 0) {
       sqlFliter = strTem1 + " where " + strTem4
     } else {
       sqlFliter = strTem1
     }
     (sqlFliter, down, up)
   }else {
     (str,"","")
   }
  }

  def  sqlTimeExract(sql:String,column:String):(String,String,String)={
    val str=caseUnify(sql)
    if (str.contains("where")) {
      val strTem = str.split("where")
      val strTem1 = strTem(0)
      val strTem2 = strTem(1).split("and").toBuffer

      var down = ""
      var up = ""

      for (k <- 0 until strTem2.length) {
        if (strTem2(k).contains(column)) {
          if (strTem2(k).contains(">")) {
            down = strTem2(k)
          }
          if (strTem2(k).contains("<")) {
            up = strTem2(k)
          }
        }
      }

      val strTem3 = strTem2.-(down, up)
      var strTem4 = ""

      for (i <- 0 until strTem3.length) {
        if (i == 0) {
          strTem4 = strTem3(0).replaceAll(" ", "")
        } else {
          strTem4 = strTem4 + " and " + strTem3(i).replaceAll(" ", "")
        }
      }

//      down = down.replaceAll(" ", "")
//      up = up.replaceAll(" ", "")

      val regex = """'(.+)'""".r
      val regex(lo)=regex.findFirstIn(down).get
      down=lo
      val regex(hi)=regex.findFirstIn(up).get
      up=hi


      var sqlFliter = ""
      if (strTem4.length != 0) {
        sqlFliter = strTem1 + " where " + strTem4
      } else {
        sqlFliter = strTem1
      }
      (sqlFliter, down, up)
    }else {
      (str,"","")
    }
  }

  // Bson 选取列表达式
  def mongoSelect(fieldTem:JsonArray):String={

    var field=""
    for(i<-0 until fieldTem.size){
      val str=fieldTem.get(i).getAsJsonObject.get("name").getAsString
      if(i==0){
        field="'"+str+"'"+":'$"+str+"'"
      }else{
        field=field+","+"'"+str+"'"+":'$"+str+"'"
      }
    }
    "{"+field+"}"
  }

  // Don't  allow  input value  is  empty
  def  monsqlCov(likeSql:String,dataTypeArr:JsonArray):String={

    // 提取条件表达式中的数据类型
    def  getType(dataTypeArr:JsonArray,column:String):String={
      var  str=""
      breakable(
        for(i<- 0  until dataTypeArr.size ){
          str= dataTypeArr.get(i).getAsString
          if(str.contains(column.replaceAll("'",""))){
            // extract  () content
            /* val  pattern="(\\([^\\)]+\\))".r
             str= (pattern findFirstIn str).get*/
            str = str.substring(str.indexOf("(")+1,str.indexOf(")"))
            break()
          }
        }
      )
      str
    }

    val   x2Tem=likeSql.split(",")
    val   len=x2Tem.length
    // split and  transform  expressions
    val arr = Array.ofDim[String](len,3)

    for(i<- 0 until len) {
      if (x2Tem(i).contains(">"))
      {
        if (x2Tem(i).contains("=")) {
          //  sign contains  ">="
          val  tem= x2Tem(i).replaceAll(">.*=", ">=").split(">=")
          arr(i)(0)="'"+tem(0)+"'"
          arr(i)(1)="'$gte'"
          arr(i)(2)=tem(1)

        } else {
          val tem = x2Tem(i).split(">")
          arr(i)(0)="'"+tem(0)+"'"
          arr(i)(1)="'$gt'"
          arr(i)(2)=tem(1)
        }

      }
      else  if(x2Tem(i).contains("<"))
      {
        if (x2Tem(i).contains("=")) {
          //  sign contains  "<="
          val  tem = x2Tem(i).replaceAll("<.*=", "<=").split("<=")
          arr(i)(0)="'"+tem(0)+"'"
          arr(i)(1)="'$lte'"
          arr(i)(2)=tem(1)
        } else {
          val  tem = x2Tem(i).split("<")
          arr(i)(0)="'"+tem(0)+"'"
          arr(i)(1)="'$lt'"
          arr(i)(2)=tem(1)
        }

      }
      else if(x2Tem(i).contains("="))
        {
          val tem=x2Tem(i).split("=+")
          arr(i)(0)=s"'${tem(0)}'"
          arr(i)(1)="'$eq'"
          arr(i)(2)=tem(1)
        }
      else
      {
        println("不支持此种语法")
      }
    }

    // Number  Date  String    if  (len>1)   classify
    val temArr= new  Array[String](len)
    for(i <- 0  until  len){
      temArr(i)=arr(i)(0).replaceAll(" ","")
    }
    //  筛选相同字段 与 行标号
    val temBuffer=new ArrayBuffer[(String,Int,Int)]()
    for(i <- 0 until  len){
      for (j <- i+1 until  len){
        if  (temArr(i) == temArr(j)){
          val  tuple=(temArr(i),i,j)
          temBuffer.append(tuple)
        }
      }
    }

    // store   same  column  rowNum
    val  rowBuffer=new ArrayBuffer[Int]()

    var  query2=""
    var  query1=""

    if (temBuffer.length>0) {
      //  consider Date  Type
      for (i <- 0 until temBuffer.length) {

        val column = temBuffer(i)._1
        val sign1 = arr(temBuffer(i)._2)(1)
        val pra1 = arr(temBuffer(i)._2)(2)
        val sign2 = arr(temBuffer(i)._3)(1)
        val pra2 = arr(temBuffer(i)._3)(2)

        //need other function determine the field data type.
        val dataType=new MongoDataType().conversion(getType(dataTypeArr,column))
        var queryTem=""
        if (dataType=="Date"){
           queryTem=s"$column:{$sign1:ISODate($pra1),$sign2:ISODate($pra2)}"
        }
        else
        {
           queryTem = s"$column:{$sign1:$pra1,$sign2:$pra2}"
        }

       // val dateQuery2 = s"$column:{$sign1:ISODate($pra1),$sign2:ISODate($pra2)}"
       //  val otherQuery2 = s"$column:{$sign1:$pra1,$sign2:$pra2}"

        if (i == 0) {
          query2 =queryTem
        }
        else {
          query2 = query2 + "," + queryTem
        }
        rowBuffer.append(temBuffer(i)._2)
        rowBuffer.append(temBuffer(i)._3)
      }

    }

    if(len-temBuffer.length>0) {

      for (j <- 0 until len) {
        var filter: Boolean = true
        for (i <- 0 until rowBuffer.length) {
          if (i == 0) {
            filter = (j != rowBuffer(0))
          }
          else {
            filter = (filter && j != rowBuffer(i))
          }
        }

        if (filter) {
          val column = arr(j)(0)
          val sign1  = arr(j)(1)
          val pra1   = arr(j)(2)

          val dataType=new MongoDataType().conversion(getType(dataTypeArr,column))

          var queryTem=""
          if (dataType=="Date"){
            if(sign1=="'$eq'")
              queryTem=s"$column:ISODate($pra1)"
            else
              queryTem=s"$column:{$sign1:ISODate($pra1)}"
          }
          else
          {
            if(sign1=="'$eq'")
              queryTem =s"$column:$pra1"
            else
              queryTem =s"$column:{$sign1:$pra1}"
          }

         /* val dateQuery1 = s"$column:{$sign1:ISODate($pra1)}"
          val otherQuery1 = s"$column:{$sign1:$pra1}"*/

          if (query1.size==0) {
            query1 = queryTem
          }
          else {
            query1 =query1 + "," + queryTem
          }
        }
      }
    }
    var  query=""
    if  (query1.size!=0  &&  query2.size==0 ) {
      query="{"+query1+"}"
    }
    else  if  (query2.size!=0 &&  query1.size==0  ){
      query="{"+query2+"}"
    }
    else{
      query="{"+query1+","+query2+"}"
    }
    query
  }

}
