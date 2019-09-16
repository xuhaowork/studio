package org.apache.spark.sql.regularExpression

import com.google.gson.JsonParser
import com.self.core.baseApp.myAPP
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

//最好就是所有的列都有配置文件给出，或者输入框输入。
object lineMain extends  myAPP{

  override  def  run():Unit= {

    val sqlc=new SQLContext(sc)
    val jsonparam="""{"columnReg":{"configUrl":"D:\\testdata\\name.txt","value":"config"},"path":"D:\\testdata\\name.txt"}"""
    /* val jsonparam=
       """{"columnReg":{"columnRegList":[{"column":"xcvbvfb","reg":"aaaaa"},{"column":"haohaohaoxuexi","reg":"bbbbb"},{"column":"tiantianxiangshang","reg":"ccccc"}],
         |"value":"handing"},"path":"/master/nnc"}""".stripMargin*/

    val  parser = new JsonParser()
    val  temJsonParser=parser.parse(jsonparam).getAsJsonObject
    // 数据文件
    val  dataPath=temJsonParser.get("path").getAsString
    println(dataPath)
    val  columnReg=temJsonParser.get("columnReg").getAsJsonObject
    val  value=columnReg.get("value").getAsString

    val pattern=new  ArrayBuffer[Regex]
    //存放列名
    val column=new  ArrayBuffer[String]  //存放列名及  列名_index, 列名_count

    if(value=="handing"){
      val  columnRegList=columnReg.get("columnRegList").getAsJsonArray
      val  length=columnRegList.size()

      for(i<- 0 until length){
        val  arrTem=columnRegList.get(i).getAsJsonObject
        val  field=arrTem.get("column").getAsString
        val  expression=arrTem.get("reg").getAsString

        column.append(field)
        column.append(field+"_index")
        column.append(field+"_count")
        val reg=new Regex(expression)
        pattern.append(reg)
      }
    } else {
      val  configUrl = columnReg.get("configUrl").getAsString
      val  configArr=sc.textFile(configUrl).map(x=>x.split(";")).collect()(0)
      for (i <- 0 until configArr.length) {
        val arrTem = configArr(i).split(",")
        column.append(arrTem(0))
        //另外加的两列。
        column.append(arrTem(0) + "_index")
        column.append(arrTem(0) + "_count") // 得是Int类型。
        val reg = new Regex(arrTem(1))
        pattern.append(reg)
      }
    }

    //设置数据类型
    val structFields = new Array[StructField](column.length)
    for(i<-0 until column.length){
      if((i+1)%3==0){
        structFields(i) = StructField(column(i),IntegerType, true)
      }
      else {
        structFields(i) = StructField(column(i),ArrayType(StringType),true)
      }
    }
    val schema=StructType(structFields)
    val rdd = sc.textFile(dataPath) //dataPath
    val newRdd = rdd.map{line=>
      var list:List[Any]=List()
      for(i<- 0 until pattern.length){
        val extract= pattern(i) findAllMatchIn(line)
        var count=0
        val contentArr=new ArrayBuffer[String]
        val indexArr=new ArrayBuffer[String]
        while(extract.hasNext){
          val tem=extract.next()
          contentArr.append(tem.toString)
          indexArr.append(tem.start+"_"+tem.end)
          count=count+1
        }
        list=list.:+(contentArr.toArray)
        list=list.:+(indexArr.toArray)
        list=list.:+(count)
      }
       Row.fromSeq(list)
    }

    val outDf=sqlc.createDataFrame(newRdd,schema)
    /*outputrdd.put("<#rddtablename#>",outDf)
    outDf.registerTempTable("<#rddtablename#>")
    sqlc.cacheTable("<#rddtablename#>")*/
    outDf.show()
  }
}
