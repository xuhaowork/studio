package com.self.core.learningStreaming

import com.self.core.baseApp.myAPP
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

object writeInfoToDB {
  def main(args: Array[String]): Unit = {
    println("good")

    import org.apache.spark.{SparkContext, SparkConf}

    import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
    import org.apache.spark.streaming.StreamingContext._

    import org.apache.spark.storage.StorageLevel
    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    ssc.checkpoint("F://")
    // 通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中

    val lines = ssc.socketTextStream("192.168.21.14", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // 对读入的数据进行分割、计数
    val words = lines.flatMap(_.split(","))


    val wordCounts = words.map(x => (x, 1))
    val result = wordCounts.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValues = state.getOrElse(0)
      values.foreach {
        value =>
          newValues += value
      }
      Some(newValues)
    })

    result.print()

    ssc.start()
    ssc.awaitTermination()


    "Array[Int]"

    Manifest


    /** 最后效果 */
//    -------------------------------------------
//    Time: 1534754520000 ms
//    -------------------------------------------
//    (Joe,6)
//    (Adam,3)
//    (﻿Ani,8)
//    (Jock,3)

//    -------------------------------------------
//    Time: 1534754540000 ms
//    -------------------------------------------
//    (Joe,5)
//    (Adam,4)
//    (﻿Ani,7)
//    (Jock,4)




  }

  def zero(): Unit = {




//    import java.sql.{Connection, DriverManager, PreparedStatement}
//    import org.apache.spark.streaming.{Seconds, StreamingContext}
//    import org.apache.spark.{SparkConf, SparkContext}
//    import org.slf4j.LoggerFactory
//
//    /**
//      * 记录最近五秒钟的数据
//      */
//    object  RealtimeCount1{
//
//      case class Loging(vtime:Long,muid:String,uid:String,ucp:String,category:String,autoSid:Int,dealerId:String,tuanId:String,newsId:String)
//
//      case class Record(vtime:Long,muid:String,uid:String,item:String,types:String)
//
//
//      val logger = LoggerFactory.getLogger(this.getClass)
//
//      def main(args: Array[String]) {
//        val argc = new Array[String](4)
//        argc(0) = "10.0.0.37"
//        argc(1) = "test-1"
//        argc(2) = "test22"
//        argc(3) = "1"
//        val Array(zkQuorum, group, topics, numThreads) = argc
//        val sparkConf = new SparkConf().setAppName("RealtimeCount").setMaster("local[2]")
//        val sc = new SparkContext(sparkConf)
//        val ssc = new StreamingContext(sc, Seconds(5))
//
//
//        val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
//
//        val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(x=>x._2)
//
//        val sql = "insert into loging_realtime1(vtime,muid,uid,item,category) values (?,?,?,?,?)"
//
//        val tmpdf = lines.map(_.split("\t")).map(x=>Loging(x(9).toLong,x(1),x(0),x(3),x(25),x(18).toInt,x(29),x(30),x(28))).filter(x=>(x.muid!=null && !x.muid.equals("null") && !("").equals(x.muid))).map(x=>Record(x.vtime,x.muid,x.uid,getItem(x.category,x.ucp,x.newsId,x.autoSid.toInt,x.dealerId,x.tuanId),getType(x.category,x.ucp,x.newsId,x.autoSid.toInt,x.dealerId,x.tuanId)))
//        tmpdf.filter(x=>x.types!=null).foreachRDD{rdd =>
//          //rdd.foreach(println)
//          rdd.foreachPartition(partitionRecords=>{
//            val connection = ConnectionPool.getConnection.getOrElse(null)
//            if(connection!=null){
//              partitionRecords.foreach(record=>process(connection,sql,record))
//              ConnectionPool.closeConnection(connection)
//            }
//          })
//        }
//        ssc.start()
//        ssc.awaitTermination()
//      }
//
//      def getItem(category:String,ucp:String,newsId:String,autoSid:Int,dealerId:String,tuanId:String):String = {
//        if(category!=null && !category.equals("null")){
//          val pattern = "http://www.ihaha.com/\\d{4}-\\d{2}-\\d{2}/\\d{9}.html"
//          val matcher = ucp.matches(pattern)
//          if(matcher) {
//            ucp.substring(33,42)
//          }else{
//            null
//          }
//        }else if(autoSid!=0){
//          autoSid.toString
//        }else if(dealerId!=null && !dealerId.equals("null")){
//          dealerId
//        }else if(tuanId!=null && !tuanId.equals("null")){
//          tuanId
//        }else{
//          null
//        }
//      }
//
//      def getType(category:String,ucp:String,newsId:String,autoSid:Int,dealerId:String,tuanId:String):String = {
//        if(category!=null && !category.equals("null")){
//          val pattern = "100000726;100000730;\\d{9};\\d{9}"
//          val matcher = category.matches(pattern)
//
//          val pattern1 = "http://www.chexun.com/\\d{4}-\\d{2}-\\d{2}/\\d{9}.html"
//          val matcher1 = ucp.matches(pattern1)
//
//          if(matcher1 && matcher) {
//            "nv"
//          }else if(newsId!=null && !newsId.equals("null") && matcher1){
//            "ns"
//          }else if(matcher1){
//            "ne"
//          }else{
//            null
//          }
//        }else if(autoSid!=0){
//          "as"
//        }else if(dealerId!=null && !dealerId.equals("null")){
//          "di"
//        }else if(tuanId!=null && !tuanId.equals("null")){
//          "ti"
//        }else{
//          null
//        }
//      }
//
//      def process(conn:Connection,sql:String,data:Record): Unit ={
//        try{
//          val ps : PreparedStatement = conn.prepareStatement(sql)
//          ps.setLong(1,data.vtime)
//          ps.setString(2,data.muid)
//          ps.setString(3,data.uid)
//          ps.setString(4,data.item)
//          ps.setString(5,data.types)
//          ps.executeUpdate()
//        }catch{
//          case exception:Exception=>
//            logger.warn("Error in execution of query"+exception.printStackTrace())
//        }
//      }
//    }
















  }
}
