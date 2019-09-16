package com.self.core.baseApp

import java.util.HashMap

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by DataShoe on 2018/1/5.
  */
abstract class myAPP {
  /** spark 2.0 */
  /*
  import org.apache.spark.sql.{SQLContext, SparkSession}
  lazy val cfg: SparkConf = new SparkConf()
    .setAppName("DataShoe_Learning")
    .setMaster("local[4]")
    .set("spark.sql.warehouse.dir", "file:///")

  lazy val sparks: SparkSession = SparkSession.builder().config(cfg).getOrCreate()

  lazy val sc: SparkContext = sparks.sparkContext
  lazy val sqlc: SQLContext = sparks.sqlContext

  lazy val hqlc = sqlc
  lazy val ssc: StreamingContext = new StreamingContext(cfg, Seconds(1))

  val outputrdd: java.util.Map[java.lang.String, java.lang.Object] =
    new util.HashMap[java.lang.String, java.lang.Object]()

  class Z4local extends Z {
    override def rdd(rddName: String): AnyRef = {
      outputrdd.get(rddName)
    }
  }

  val z: Z = new Z4local

  def run(): Unit

  def main(args: Array[String]): Unit = {
    run()
    sparks.stop()
  }
  */

  /** spark 1.6 */
  lazy val conf = new SparkConf().setMaster("local[4]").setAppName("DataShoe-Learning")
  lazy val sc = new SparkContext(conf)
  lazy val sqlc = new SQLContext(sc)
  lazy val hqlc = sqlc
  lazy val smc: StreamingContext = new StreamingContext(conf, Seconds(60))
  lazy val memoryMap: java.util.Map[java.lang.String, java.lang.Object] = new HashMap[java.lang.String, java.lang.Object]()
  lazy val outputrdd: java.util.Map[java.lang.String, java.lang.Object] = new HashMap[java.lang.String, java.lang.Object]();

  class Z4local extends Z {
    override def rdd(rddName: String): AnyRef = {
      outputrdd.get(rddName)
    }
  }

  val z: Z = new Z4local

  def run(): Unit

  def main(args: Array[String]): Unit = {
    run()
  }
}
