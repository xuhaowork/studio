package org.apache.spark.sql.dbPartiiton

import java.sql.ResultSet

import com.google.gson.JsonParser
import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.utils.{NumericType, ToolUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.dbPartiiton.partition.Analysis._
import org.apache.spark.sql.dbPartiiton.partition.Partitioner
import org.apache.spark.sql.dbPartiiton.service.{ConnectionService, ResConx}
import org.apache.spark.sql.dbPartiiton.dataType.TypeConverFactory._
import org.apache.spark.sql.dbPartiiton.jdbc.ToDf


/**
  * editor: baiyuan
  * date: 2017.12.28
  **/

object PartitionMain extends BaseMain {

  override def run(): Unit = {

    /*
    *   you  need to modify the database type case
    * */
    //行分区
    /*    val   jsonparam =""" {"PartitionType":{"value":"row","PartitionsNum":"120"},"DBType":{"value":"MySQL"},
         "Host":"192.168.11.26","Port":"3306","User":"oozie","Passw":"oozie",
         "ConnType":"Host","action":"queryTablesList.rest","dte":1514358988445,
         "Base":"0.8a","Table":"bundle","Sql":"select * from bundle",
         "PartitionsNum":"120","FetchSize":"30000","ThreadCount":"2"}"""*/

    //列分区: 手动分区

    /*  val jsonparam =
        """{"PartitionType":{"value":"column","PartitionWay":{"value":"false","Partition":"id<100,
          |id>=100 and  id<=1000"}},"DBType":{"value":"MySQL"},"Host":"192.168.11.26","Port":"3306",
          |"User":"oozie","Passw":"oozie","ConnType":"Host","action":"queryTablesList.rest",
          |"dte":1514358988445,"Base":"0.8a","Table":"bundle","Sql":"select * from bundle",
          |"PartitionsNum":"120","FetchSize":"30000","ThreadCount":"2"}""".stripMargin
*/
    // Gbase8t 数据库  行分区
    val jsonparam =
    """ {"PartitionType":{"value":"row","PartitionsNum":"3"},
      |"DBType":{"value":"Gbase8t","base":"zzjzweb","Sid":"gbase1"},
      |"Host":"192.168.11.27","Port":"9088","User":"informix","Passw":"informix",
      |"ConnType":"Host","action":"queryTablesList.rest","dte":1514358988445,"Base":"zzjzweb",
      |"Table":"bundle","Sql":"select * from bundle","PartitionsNum":"120","FetchSize":"30000","ThreadCount":"2"}""".stripMargin

    // oracle 数据库 行分区

    /* val jsonparam =
       """{"PartitionType":{"value":"row","PartitionsNum":"120"},
         |"DBType":{"value":"Oracle","Sid":"ORCL"},"Host":"192.168.11.251",
         |"Port":"1521","User":"scott","Passw":"zzjzorcl","ConnType":"Host","action":
         |"queryTablesList.rest","dte":1514358988445,"Base":"SCOTT","Table":"airRoute2","Sql":
         |"select * from T_WIDEDF_FAIRRESULTSINFO ","PartitionsNum":"120","FetchSize":"30000","ThreadCount":"2"}""".stripMargin*/


    val parser = new JsonParser()
    val temJsonParser = parser.parse(jsonparam).getAsJsonObject
    val pJsonParser = temJsonParser.getAsJsonObject("PartitionType")

    val jsonDBType = temJsonParser.get("DBType").getAsJsonObject
    val dbType = jsonDBType.get("value").getAsString

    var sid = ""

    if (dbType == "Gbase8t" || dbType == "Oracle") {
      sid = jsonDBType.get("Sid").getAsString
    }

    val host = temJsonParser.get("Host").getAsString
    val port = temJsonParser.get("Port").getAsString
    val user = temJsonParser.get("User").getAsString
    val passw = temJsonParser.get("Passw").getAsString
    val base = temJsonParser.get("Base").getAsString
    val sql = temJsonParser.get("Sql").getAsString
    //    val  fetchSize=temJsonParser.get("FetchSize").getAsInt
    //    val  threadCount=temJsonParser.get("ThreadCount").getAsInt
    // 数据校验
    val fetchSize = ToolUtils.toSignType[Int](temJsonParser.get("FetchSize").getAsString)(NumericType.POSITIVE)
    val threadCount = ToolUtils.toSignType[Int](temJsonParser.get("ThreadCount").getAsString)(NumericType.POSITIVE)
    //later, need  modification

    var outDf: DataFrame = null
    val service = new ConnectionService()
    val driver = service.paths.get(dbType)

    println(driver)
    val url = service.getUrl(dbType, host, port, base, sid)
    println(url)

    val partitionType = pJsonParser.get("value").getAsString
    // 按列字段分区
    if (partitionType == "column") {
      val partitionWay = pJsonParser.getAsJsonObject("PartitionWay")
      // 列分区自动
      if (partitionWay.get("value").getAsString == "true") {

        val temField = partitionWay.get("Field").getAsJsonArray.get(0).getAsJsonObject

        println(temField)

        var field = temField.get("name").getAsString
        var dataType = temField.get("datatype").getAsString
        //将所有分区字段的数据类型映射成归类后的数据类型。
        dataType = conver(dbType, dataType)


        println(dataType)
        ////////////////////////
        //        val  simpleCount=partitionWay.get("simpleCount").getAsInt
        //        val  partitionsNum=partitionWay.get("PartitionsNum").getAsInt
        val simpleCount = ToolUtils.toSignType[Int](partitionWay.get("simpleCount").getAsString)(NumericType.POSITIVE)
        val partitionsNum = ToolUtils.toSignType[Int](partitionWay.get("PartitionsNum").getAsString)(NumericType.POSITIVE)

        try {
          // 如果分区字段，需要加符号时的处理，如：oracle有字段小写时，需要加"",不需要的填的话，请务必什么也不写。
          val fieldSymbol = partitionWay.get("FieldSymbol").getAsString
          field = fieldSymbol
        } catch {
          case e: Exception =>
        }

        //用来存放每个分区的查询条件
        var partition1 = new Array[String](partitionsNum)
        //将sql中的关键字转化为小写及提取筛序条件
        val tuple = sqlExract(sql, field)
        //同一成写后的查询语句
        val sql2 = tuple._1
        //筛选条件下限，没有为空
        val down = tuple._2
        //筛选条件上限，没有为空
        val up = tuple._3
        //连接数据库，查询分区字段数据集

        val conn = service.getConnection(driver, url, user, passw)
        val stmt = conn.createStatement()
        //val sql1 = sql.replaceAll("select.*from", "select " + field + " from")
        val sql1 = sql.replaceFirst("select.+?from", "select " + field + " from")
        println(sql1)
        val rs = stmt.executeQuery(sql1)
        //  consider  partitionsNum=1
        //根据分区字段的数据类型，选取对应的抽样方法
        if (dataType == "Long") {
          //对分区字段的数据集用蓄水池抽样算法进行抽样
          val sam = new Partitioner().reservoirSampleLong(rs, simpleCount * partitionsNum)
          partition1 = new Partitioner().longPart(down, up, sam, partitionsNum, field)
        } else if (dataType == "Double") {
          val sam = new Partitioner().reservoirSampleDouble(rs, simpleCount * partitionsNum)
          partition1 = new Partitioner().doublePart(down, up, sam, partitionsNum, field)
        } else {
          val sam = new Partitioner().reservoirSampleString(rs, simpleCount * partitionsNum)
          partition1 = new Partitioner().stringPart(down, up, sam, partitionsNum, field)
        }
        rs.close()
        stmt.close()
        conn.close()

        outDf = new ToDf(sc, sqlc).jdbc(url, driver, user, passw, sql2, partition1, fetchSize, threadCount)


      } else { //列分区手动
        //将sql中的关键字转化为小写
        val sql2 = caseUnify(sql)
        val partition = partitionWay.get("Partition").getAsString
        val partition1 = partition.split(",")
        outDf = new ToDf(sc, sqlc).jdbc(url, driver, user, passw, sql2, partition1, fetchSize, threadCount)

      }

    } else {
      // 行分区
      //      val partitionsNum = pJsonParser.get("PartitionsNum").getAsInt
      val partitionsNum = ToolUtils.toSignType[Int](pJsonParser.get("PartitionsNum").getAsString)(NumericType.POSITIVE)
      //连接数据库，得到要查询的数据的总行数
      val conn = service.getConnection(driver, url, user, passw)
      val stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      //      val rs = stmt.executeQuery(sql)
      //      rs.last()
      //      val  up=rs.getRow
      val sql2 = sql.replaceFirst("select.+?from", "select " + "count(1) as cnt" + " from")
      val rs = stmt.executeQuery(sql2)
      rs.first()
      var up = rs.getLong(1)
      rs.close
      stmt.close
      conn.close

      var fqNum = partitionsNum
      if (up == 0) {
        fqNum = 1
        if (dbType == "Gbase8t") {
          up = 1
        }
      }
      outDf = new ToDf(sc, sqlc).rowNumJdbc(url, driver, user, passw, dbType, sql, 0, up, fqNum, fetchSize, threadCount)
      //      outDf=new ToDf(sc,sqlc).rowNumJdbc(url,driver,user,passw,dbType,sql,0,up,partitionsNum,fetchSize,threadCount)

    }

    /*outputrdd.put("<#rddtablename#>",outDf)
    outDf.registerTempTable("<#rddtablename#>")
    sqlc.cacheTable("<#rddtablename#>")*/

    outDf.show()

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
