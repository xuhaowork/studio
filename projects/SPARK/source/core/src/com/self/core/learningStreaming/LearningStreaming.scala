package com.self.core.learningStreaming

import com.self.core.baseApp.myAPP
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object LearningStreaming extends myAPP{
  def test() = {
    import org.apache.spark._
    import org.apache.spark.streaming._
    import java.util
    import java.util.Properties
    import org.apache.spark.SparkContext
    import scala.collection.JavaConverters._
    import org.apache.spark.rdd.RDD
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    import org.apache.spark.streaming.dstream.ReceiverInputDStream

    import org.apache.spark.streaming.dstream.ReceiverInputDStream


    type dsType = {
      def run(todo : Runnable):Unit;
      def doState(cmdnum : Int):Unit;
      def fromJson(jsonStr : String):scala.collection.mutable.Map[String, String];
      def registry(ssc : org.apache.spark.streaming.StreamingContext) : Unit;
      def getStreamContext() : org.apache.spark.streaming.StreamingContext;
      def isDebug():Boolean;
      def delSelf():Unit;

      def clearRunner():Unit;
      def add(elem: Runnable): Seq[Runnable];
      def run() : Unit;
    }

    val z1 = outputrdd
    val dsCtl: dsType = z1.get("dsCtl").asInstanceOf[dsType]
    val ssc = dsCtl.getStreamContext

     ssc.checkpoint("./testStreaming/")

    val st: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.21.14", 9999)

    val inputData = st.map(x => (x, 1))

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum // 该key对应的所有值

      val previousCount = state.getOrElse(0)

      Some(currentCount + previousCount)
    }

    val result = inputData.updateStateByKey(updateFunc).map { case (key, count) => key + "," + count }
    val rddTableName = "<#zzjzRddName#>"

    result.foreachRDD(
      rdd => {
        val df = sqlc.createDataFrame(rdd.map(str => Row(str)), StructType(Array(StructField("name", StringType))))
        outputrdd.put("someTest", df)
      }
    )

    outputrdd.put("uuuu", result)

  }

  override def run(): Unit = {
//    println("good")
//
//    /**  通过文件创建DStream，要求文件的时间戳要晚于每次执行时的时间戳 */
//    val lines = smc.textFileStream("F://myStudio/data/streaming/")
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x=> (x, 1)).reduceByKey(_ + _)
//
//    wordCounts.print()
//
//    smc.start()
//
//    smc.awaitTermination()
//
//
//    smc.socketTextStream("192.168.21.11", 9999)


    import org.apache.spark._
    import org.apache.spark.streaming._
    import java.util
    import java.util.Properties
    import org.apache.spark.SparkContext
    import scala.collection.JavaConverters._

    type dsType = {
      def run(todo : Runnable):Unit;
      def doState(cmdnum : Int):Unit;
      def fromJson(jsonStr : String):scala.collection.mutable.Map[String, String];
      def registry(ssc : org.apache.spark.streaming.StreamingContext) : Unit;
      def getStreamContext() : org.apache.spark.streaming.StreamingContext;
      def isDebug():Boolean;
      def delSelf():Unit;

      def clearRunner():Unit;
      def add(elem: Runnable): Seq[Runnable];
      def run() : Unit;
    }

    val dsCtl: dsType = outputrdd.get("dsCtl").asInstanceOf[dsType]
//    val ssc = dsCtl.getStreamContext
//
//    ssc.checkpoint("./testStreaming/")
//
//

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(60))
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.21.14", 9999)

    val inputData = dStream.map(x => (x, 1))
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val result = inputData.updateStateByKey(updateFunc).map { case (key, count) => key + "," + count }
    val rddTableName = "<#zzjzRddName#>"

    result.foreachRDD {
      rdd: RDD[String] =>
    }

    outputrdd.put(rddTableName, result)

    ssc.start()
    ssc.awaitTermination()

    import org.apache.spark.mllib.clustering.KMeans




  }
}
