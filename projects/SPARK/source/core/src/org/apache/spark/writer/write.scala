package org.apache.spark.sql.writer

import java.util.Properties

import com.google.gson.JsonParser
import com.zzjz.deepinsight.basic.BaseMain
import org.apache.spark.sql.dbPartiiton.service.{ConnectionService, ResConx}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.writer.SchemaWriter._
import org.apache.spark.sql.writer.JdbcUtils._

/**
  * Author:  BaiYuan
  * Date:    2018/1/29
* */
object write  extends  BaseMain{
  override def run:Unit={

   /* //  Gbase8t
    val jsonparam =
      """{"inputTableName":"文本数据源_1复件1_Kq5C9Y","DBType":
        |{"value":"Gbase8t","base":"zzjzweb","Sid":"gbase1"},
        |"Host":"192.168.11.27","Port":"9088","User":"informix","Passw":"informix",
        |"writeMethod":{"value":"createTable","newTableName":"zzz"},
        |"ConnType":"Host","Base":"zzjzweb"}""".stripMargin*/

   /* // Oracle:
    val jsonparam ="""{"inputTableName":"文本数据源_1复件1_Kq5C9Y",
                  "DBType":{"value":"Oracle","Sid":"ORCL"},
                  "Host":"192.168.11.251","Port":"1521","User":"scott","Passw":"zzjzorcl",
                  "writeMethod":{"value":"selectFromDB"},
                  "ConnType":"Host","Base":"SCOTT","Table":"bank","currentPage":1}"""*/

  //自己输表名
   /* val jsonparam =
      """{"inputTableName":"快读数据库_1复件1_ZQLXxo","DBType":{"value":"Gbase8t","base":"zzjzweb","Sid":"gbase1"},
        |"Host":"192.168.11.27","Port":"9088","User":"informix","Passw":"informix","writeMethod":{"value":"selectFromDB"},
        |"ConnType":"Host","Base":"zzjzweb","Table":"bundle",
        |"columnsMapping":[{"column":"id","name":"FIELD_0","datatype":"string"},
        |{"column":"bundle_name","name":"FIELD_1","datatype":"string"},
        |{"column":"coordinator_id","name":"FIELD_1","datatype":"string"},{"column":"create_date","name":"FIELD_2","datatype":"string"},
        |{"column":"status","name":"FIELD_0","datatype":"string"}]}""".stripMargin*/


    val jsonparam =
      """ {"inputTableName":"快读数据库_1复件1_ZQLXxo","DBType":{"value":"Gbase8t","base":"zzjzweb","Sid":"gbase1"},
        |"Host":"192.168.11.27","Port":"9088","User":"informix","Passw":"informix","writeMethod":{"value":"selectFromDB"},
        |"ConnType":"Host","Base":"zzjzweb","Table":"boolean","columnsMapping":[{"column":"a","name":"boolean","datatype":"string"}],
        |"threadCount":"2"}""".stripMargin


   /* // hive 新建表
    val jsonparam =
      """{"inputTableName":"文本数据源_1复件1_Kq5C9Y",
        |"DBType":{"value":"Hive"},
        |"Host":"192.168.11.51","Port":"10000","User":"root","Passw":"root",
        |"writeMethod":{"value":"createTable","newTableName":"zz"},
        |"ConnType":"Host","Base":"test","Table":"bank","currentPage":1}""".stripMargin*/

   /* // hive 选表
    val jsonparam =
      """{"inputTableName":"文本数据源_1复件1_Kq5C9Y","DBType":{"value":"Hive"},
        |"Host":"192.168.11.51","Port":"10000","User":"root","Passw":"root",
        |"writeMethod":{"value":"selectFromDB"},"ConnType":
        |"Host","Base":"test","Table":"xxx","currentPage":1}""".stripMargin*/

    //val jsonparam = """<#zzjzParam#>"""
    val  parser = new JsonParser()
    val  temJsonParser=parser.parse(jsonparam).getAsJsonObject
    val  jsonDBType=temJsonParser.get("DBType") .getAsJsonObject
    val  inputTableName=temJsonParser.get("inputTableName").getAsString
    val  dbType= jsonDBType.get("value").getAsString
    var  sid=""
    if(dbType=="Gbase8t" || dbType=="Oracle") { sid=jsonDBType.get("Sid").getAsString}
    val  host=temJsonParser.get("Host").getAsString
    val  port=temJsonParser.get("Port").getAsString
    val  user=temJsonParser.get("User").getAsString
    val  passw=temJsonParser.get("Passw").getAsString
    val  base=temJsonParser.get("Base").getAsString

    val  threadCount=temJsonParser.get("threadCount").getAsInt

    val writeMethodTem=temJsonParser.getAsJsonObject("writeMethod")
    val writeMethod=writeMethodTem.get("value").getAsString
    var writeTable=""

  /*  //mysql 数据源
    val  sqlc:SQLContext=new SQLContext(sc)
    val prop1 = new Properties()
    prop1.setProperty("user","oozie")
    prop1.setProperty("password","oozie")
    val  url1="jdbc:mysql://192.168.11.26:3306/ceshi"
    val  df= sqlc.read.jdbc(url1, "(select * from  baiyuanType) as aa", prop1)

    df.printSchema()
    println(df.schema.simpleString)*/

    var df:DataFrame=null

    if(writeMethod=="selectFromDB") {
      val column = temJsonParser.getAsJsonArray("columnsMapping")
      val len = column.size
      val arr = new Array[String](len)
      for (i <- 0 until len) {
        arr(i) = column.get(i).getAsJsonObject.get("name").getAsString
      }
      val fieldList = arr.mkString(",")
      val sql = "select " + fieldList + " from " + inputTableName
      df = sqlc.sql(sql)
    } else {
      df=z.rdd(inputTableName).asInstanceOf[DataFrame]
    }
    // 直接的DF

    dbType match {
      /*case "MySQL" => {
      val service = new ConnectionService()
      val url1 = service.getUrl (dbType, host, port, base, sid)
      val driver = service.paths.get (dbType)

      val  batchSize = temJsonParser.get("batchSize").getAsString
      val pro = new Properties()
      pro.setProperty("user", user);// 设置用户名
      pro.setProperty("password", passw);// 设置密码
      pro.setProperty("batchsize", batchSize)
      val url = s"$url1&rewriteBatchedStatements=true"

      if(writeMethod=="selectFromDB") {
        writeTable=temJsonParser.get("Table").getAsString
      }else{
        writeTable=writeMethodTem.get("newTableName").getAsString
        createTable(df,dbType,driver,url,user,passw,writeTable)
      }
      //增加线程池连接数
      import org.apache.spark.sql.writer.JdbcUtilsTest.saveTable1
      saveTable1(df, url, writeTable, pro, driver, user, passw, threadCount)

      //df.write.mode (SaveMode.Append).jdbc (url, writeTable, prop)
      // 关闭连接池
      val myrdd = sc.makeRDD(Seq(1))
      sc.runJob(myrdd, (it: Iterator[Int]) => {
        try {
          val resConx = ResConx.getInstance().conx(url)
          resConx.close()
        } catch {
          case e: Throwable => 2
        }
      })
    }*/

      case "Hive" =>

      //平台上需要加上这行代码
       /*if (!sqlc.equals(hqlc)) {
         df = DataFrameUtil.switchDataFrameContext(df, hqlc)
       }*/
        df.registerTempTable("table1")
        hqlc.sql(s"use $base")
        if(writeMethod=="selectFromDB") {
          writeTable=temJsonParser.get("Table").getAsString
          hqlc.sql(s"insert into $writeTable  select * from table1")
        }else{
          writeTable=writeMethodTem.get("newTableName").getAsString
          hqlc.sql(s"create table $writeTable as select * from table1")
        }

      case _ =>
        val service = new ConnectionService()
        val url = service.getUrl (dbType, host, port, base, sid)
        val driver = service.paths.get (dbType)


        if(writeMethod=="selectFromDB") {
          writeTable=temJsonParser.get("Table").getAsString
        }else{
          writeTable=writeMethodTem.get("newTableName").getAsString
          createTable(df,dbType,driver,url,user,passw,writeTable)
        }
        //增加线程池连接数
          saveTable(df, url, writeTable,driver,user,passw,threadCount)

        //df.write.mode (SaveMode.Append).jdbc (url, writeTable, prop)

        val myrdd = sc.makeRDD(Seq(1))
        sc.runJob(myrdd, (it: Iterator[Int]) => {
          try {
            val resConx = ResConx.getInstance().conx(url)
            resConx.close()
          } catch {
            case e: Throwable => 2
          }
        })

    }

  }

}
