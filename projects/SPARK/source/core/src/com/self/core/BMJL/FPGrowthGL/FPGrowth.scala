package com.self.core.BMJL.FPGrowthGL

import com.self.core.baseApp.myAPP

/**
  * Created by DataShoe on 2018/1/9.
  */
object FPGrowth extends myAPP{
  override def run(): Unit = {
    import org.apache.spark.mllib.fpm.FPGrowth
    import org.apache.spark.rdd.RDD

    // 数据
    val data = sc.textFile("F:/learning-workplace/learningProject/lib/spark-1.6.0/data/mllib/sample_fpgrowth.txt")
    data.foreach(println)
    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))


    //
    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

//    model.freqItemsets.collect().foreach { itemset =>
//      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
//    }

    // 频繁项
    model.freqItemsets.map(x => (x.items.mkString("[", ",", "]"), x.freq)).foreach(println)

    val minConfidence = 0.1


//    model.generateAssociationRules(minConfidence).map(x => x.antecedent)
//
//
//    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
//      println(
//        rule.antecedent.mkString("[", ",", "]")
//          + " => " + rule.consequent .mkString("[", ",", "]")
//          + ", " + rule.confidence)
//    }


















  }
}
