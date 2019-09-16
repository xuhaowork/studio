package cn.datashoe

import java.util

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * A Main to run Camel with MyRouteBuilder
  */
abstract class SparkRouteMain extends MyRouteMain {
  /** configs */
  lazy val cfg: SparkConf = new SparkConf()
    .setAppName("DataShoe_Learning")
    .setMaster("local[*]")
    .set("spark.sql.warehouse.dir", SparkMemory.HADOOP_HOME_DIR)

  /** SparkContext and sqlContext */
  lazy val sparks: SparkSession = SparkSession.builder().config(cfg).getOrCreate()
  val spark: SparkSession = sparks

  lazy val sc: SparkContext = sparks.sparkContext
  lazy val sqlc: SQLContext = sparks.sqlContext

  lazy val ssc: StreamingContext = new StreamingContext(cfg, Seconds(1))

}



