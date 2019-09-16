package org.apache.spark.sql.dbPartiiton

import java.sql.DriverManager

import com.google.gson.Gson
import com.zzjz.deepinsight.basic.BaseMain
import org.apache.spark.sql.dbPartiiton.partition.Partitioner
import org.apache.spark.sql.dbPartiiton.service.{ConnectionService, ResConx}
import org.apache.spark.sql.dbPartiiton.jdbc._


object MppTime extends BaseMain{
  override def run(): Unit= {
/*    val jsonparam =
      """{"DBType":"Oracle","ConnType":"Host","Host":"192.168.11.251","Port":"1521","User":"scott","Passw":"zzjzorcl","Sid":"ORCL",
        |"Base":"SCOTT","Table":"T_WIDEDF_FAIRRESULTSSIGNAL","PartitionType":"rownum","Field":"FAIRTIME","DataType":"Long","Sample":"true","Partition":"",
        |"Sql":"select *  from T_WIDEDF_FAIRRESULTSSIGNAL","Down":"0","Up":"131397126297386766","PartitionsNum":"5","SampleCount":"60","FetchSize":"10000","ThreadCount":"2"}""".stripMargin*/

   val jsonparam ="""{"DBType":"MppDB","Host":"109.8.252.38","Port":"25308","User":"zzjz","Passw":"zzjz@123","Sid":"ORCL","Base":"md_inmarsat","Table":"metadata_psfile",
                     "PartitionType":"rownum","Sql":"select *  from  metadata_psfile","Field":"retstatim","DataType":"Long","Sample":"true","SampleCount":"60","Down":"2017-10-08 01:00:00","Up":"2017-11-29 23:59:59",
                     |"PartitionsNum":"3","Circle":"3","FetchSize":"10000","ThreadCount":"2"}""".stripMargin


    /*val jsonparam =
      """{"DBType":"GBase8t","ConnType":"Host","Host":"192.168.11.27","Port":"9088","User":"informix","Passw":"informix","Sid":"gbase1",
        |"Base":"zzjzweb","Table":"bai1w","PartitionType":"rownum","Field":"id","DataType":"Long","Sample":"true",
        |"Sql":"select *  from bai1w","Down":"1","Up":"2000","PartitionsNum":"5"}""".stripMargin
*/
    val gson = new Gson()
    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])

    val  DBType=p.get("DBType")
    val  host=p.get("Host")
    val  port=p.get("Port")
    val  user=p.get("User")
    val  passw=p.get("Passw")
    val  sid=p.get("Sid")
    val  base=p.get("Base")
    val  Table=p.get("Table")
    val  partitionType=p.get("PartitionType")
    val  field=p.get("Field")
    val  dataType=p.get("DataType")
    // var  partition=p.get("Partition").split(",")
    val  sample=p.get("Sample")
    val  sampleCount=p.get("SampleCount").toInt
    var  sql=p.get("Sql")
    val  down=p.get("Down")
    val  up=p.get("Up")
    val  partitionsNum=p.get("PartitionsNum").toInt
    val  fetchSize=p.get("FetchSize").toInt
    val  threadCount=p.get("ThreadCount").toInt


    val  url="jdbc:postgresql://"+host+":"+port+"/"+base
    println(url)

      var partition2=new Array[String](partitionsNum+1)

      if (partitionType=="true") {
          val sql1 = sql.replaceAll("\\*", field)
          println(sql1)
          Class.forName("org.postgresql.Driver")
          val conn = DriverManager.getConnection( url, user, passw)
          val stmt = conn.createStatement()
          val rs = stmt.executeQuery(sql1)
          val sam = new Partitioner().reservoirSampleString(rs, 60 * partitionsNum)
          partition2 = new Partitioner().sampleStringPartition(down, up, sam, partitionsNum)
      }  else  {
          partition2=new  Partitioner().timePartiton(down,up,partitionsNum)
      }
    partition2.foreach(println)

    val outDf = new ToDf(sc, sqlc).jdbcString(url, "org.postgresql.Driver", user, passw, field, sql, partition2, fetchSize, threadCount)
    outDf.show()
    outDf.printSchema()

    val myrdd = sc.makeRDD(Seq(1))
    sc.runJob(myrdd, (it: Iterator[Int]) => {
      try {
        val resConx = ResConx.getInstance().conx(url)
        resConx.close()
      } catch {
        case e: Throwable => 2
      }
    })
    new org.apache.log4j.helpers.SyslogQuietWriter(new org.apache.log4j.helpers.SyslogWriter("109.8.252.94:514"), 600,
      new org.apache.log4j.varia.FallbackErrorHandler()).write("pool size:" +   ResConx.getInstance().conx(url).count())
  }
}

