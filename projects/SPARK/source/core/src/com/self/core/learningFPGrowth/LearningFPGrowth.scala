package com.self.core.learningFPGrowth

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel, FPTree}
import org.joda.time.DateTime

object LearningFPGrowth extends myAPP{
  override def run(): Unit = {

//    // 读取样本数据并解析
//    val dataRDD = sc.textFile("hdfs://master:9000/ml/data/sample_fpgrowth.txt")
//    val exampleRDD = dataRDD.map(_.split(" ")).cache()
//
//    // 建立FPGrowth模型,最小支持度为0.4
//    val minSupport = 0.4
//    val numPartition = 10
//    val model = new FPGrowth().
//      setMinSupport(minSupport).
//      setNumPartitions(numPartition).
//      run(exampleRDD)
//
//    // 输出结果
//    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")
//    model.freqItemsets.collect().foreach { itemset =>
//      println(itemset.items.mkString("[", ",", "]") + ":" + itemset.freq)
//    }

    val dt = new DateTime()
    println(dt.toString("yyyy-MM-dd w")) //dt.toString("EEE", Locale.FRENCH)






  }
}
