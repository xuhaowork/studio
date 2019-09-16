package cn.datashoe.sparkBase

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark创建APP的快捷方法
  */
abstract class SparkAPP extends App {
  /** configs */
  lazy val cfg: SparkConf = new SparkConf()
    .setAppName("DataShoe_Learning")
    .setMaster("local[*]")
    .set("spark.sql.warehouse.dir", Memory.MY_WORKPLACE_SPARK + "\\conf\\spark_dir")

  System.setProperty("hadoop.home.dir", Memory.MY_WORKPLACE_SPARK + "\\conf\\hadoop_dir")

  /** SparkContext and sqlContext */
  lazy val sparks: SparkSession = SparkSession.builder().config(cfg).getOrCreate()
  lazy val spark: SparkSession = sparks
  lazy val sc: SparkContext = sparks.sparkContext
  lazy val sqlc: SQLContext = sparks.sqlContext

  /** StreamingContext */
  lazy val ssc: StreamingContext = new StreamingContext(cfg, Seconds(1))

  def run(): Unit

  override def main(args: Array[String]): Unit = {
    run()
  }

}
