package com.self.core.fastICA

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.mllib.feature.fastICA
import scala.math

/**
  * Created by dell on 2018/2/22.
  */
object ICA extends myAPP{
  override def run(): Unit = {
    /** 构造4种常见信号波形, 20000条数据. */
    val data = Array.range(0, 20000)

    val dataF = data.map(d => {
      // 第一种，正弦数据，每200条一个周期
      val d1 = 50*scala.math.sin(scala.math.Pi * 2* d / 200)
      // 第二种，锯齿数据，每150一个周期
      val d2 = {
        val phase = d % 120
        if(scala.math.abs(phase - 60) <= 5){
          10.0 * (60 - phase)
        }else if(d == 10){
          null
        }else if(scala.math.abs(phase - 60) <= 55){
          50.0
        }else if((phase - 60) < -55 ){
          10.0 *phase
        }else{
          10.0*(phase - 120)
        }
      }
      // 第三种，城墙形状，每120一个周期
      val d3 = {
        val phase = d % 120
        if(scala.math.abs(phase - 60) <= 5){
          10 * (60 - phase)
        }else if(scala.math.abs(phase - 60) <= 55){
          50
        }else if((phase - 60) < -55 ){
          10 *phase
        }else{
          10*(phase - 120)
        }
      }
      (d1, d2, d3)
    })

    // 第四种，每10个点组成一个随机游走.
    val rd = new java.util.Random(123L)
    val data4 = Array.fill(200)({
      var startNum = 0.0
      for(i <- 0 until 10) yield {
          startNum += rd.nextGaussian()*10
          startNum
        }
    }).flatten

    val lst = dataF.zip(data4).map(tup => (tup._1._1, tup._1._2, tup._1._3.toDouble, tup._2))

    val rdd = sc.parallelize(lst)

    val rawDataFrame = sqlc.createDataFrame(rdd.map(Row.fromTuple(_)),
      StructType(Array(StructField("firstSignal", DoubleType),
        StructField("secondSignal", DoubleType),
        StructField("thirdSignal", DoubleType),
        StructField("fourthSignal", DoubleType)
      )))


    /** 对DataFrame和参数进行一些必要的判断，将DataFrame转化为RDD[Vector]. */
    val selectAll = "true"
    val selectColumn = "firstSignal, secondSignal, thirdSignal, fourthSignal"

    var colNames = Array.empty[String]
    val rawDataDF: DataFrame = selectAll match {
      case "true" => {
        colNames = rawDataFrame.schema.fieldNames
        try {
          rawDataFrame.select(colNames.map(col(_).cast(DoubleType)): _*)
        }catch{
          case e: Exception => throw new Exception("列类型不能转化为数值类型.")
        }
      }
      case "false" => {
        colNames = selectColumn.split(",").map(_.trim)
        try {
          rawDataFrame.select(colNames.map(col(_).cast(DoubleType)): _*)
        }catch{
          case e: Exception => throw new Exception("列类型不能转化为数值类型.")
        }
      }
    }

    rawDataDF.show()

    val rddVector: RDD[Vector] = rawDataDF.rdd.map(r => {
      val arr = colNames.map(r.getAs[Double])
      Vectors.dense(arr)
    })


    /** 一些参数设定. */
    val componentNums = "3"
    val whiteWithPCA = "true"
    val PCANums = "3"
    val seedNum = "123"
    val sampleFraction = "0.8"
    val alphaNum = "1.5"
    val maxIterations = "200"
    val thresholdRank = "4"

    val n: Int = colNames.length

    val m: Int = try{
      math.abs(componentNums.toInt)
    }catch{
      case e: Exception => throw new Exception("您输入的ICA成份数无法转为正整数")
    }

    val p = whiteWithPCA match {
      case "true" => try{
        math.abs(PCANums.toInt)
      }catch{
        case e: Exception => throw new Exception("您输入的PCA成份数无法转为正整数")
      }
      case "false" => n
    }

    if(p > n )
      throw new Exception("您输入的主成分个数不应该大于选定的列数。")
    if(m > p)
      throw new Exception("您输入的ICA成份个数不应该大于选定的列数和主成分数。")


    var fraction = 1.0

    if(sampleFraction != "" || sampleFraction != null){
      fraction = try{
        sampleFraction.toDouble
      }catch{
        case e: Exception => throw new Exception("输入的求期望数据占比不能转为数值类型。")
      }
    }

    if(fraction <= 0.0 || fraction > 1.0){
      throw new Exception("输入的求期望数据占比需要在0和1之间。")
    }


    var alpha = 1.5
    if(alphaNum != "" || alphaNum != null){
      alpha = try{
        alphaNum.toDouble
      }catch{
        case e: Exception => throw new Exception("输入的双曲正切函数系数不能转为数值类型。")
      }
    }

    if(alpha < 1.0 || alpha > 2.0){
      throw new Exception("输入的双曲正切函数系数需要在1和2之间。")
    }

    var maxIter = 200
    if(maxIterations != "" || maxIterations != null){
      maxIter = try{
        maxIterations.toInt
      }catch{
        case e: Exception => throw new Exception("输入的最大迭代次数不能转为数值类型。")
      }
    }


    val threshold = thresholdRank match {
      case "3" => 1E-3
      case "4" => 1E-4
      case "5" => 1E-5
      case "6" => 1E-6
      case "7" => 1E-7
      case "8" => 1E-8
      case "9" => 1E-9
      case "10" => 1E-10
    }


    val fastICAObj = new fastICA()
      .setComponetNums(m).setWhiteMatrixByPCA(p)

    val ICAObj = if(seedNum != "" || seedNum != null){
      val seed = try{
        seedNum.toLong
      }catch{
        case e: Exception => throw new Exception("输入的随机数种子不能转为长整型。")
      }

      fastICAObj
        .setAlpha(alpha)
        .setMaxIterations(maxIter)
        .setSampleFraction(fraction)
        .setThreshold(threshold)
        .setSeed(seed)
    }else{
      fastICAObj
        .setAlpha(alpha)
        .setMaxIterations(maxIter)
        .setSampleFraction(fraction)
        .setThreshold(threshold)
    }


    val whiteModel = ICAObj.fit(rddVector)
    val PCAMatrix = Array.tabulate(p, n)((i, j) => whiteModel.PCAMatrix.apply(i, j))
    val ICAMatrix = Array.tabulate(m, p)((i, j) => whiteModel.ICAMatrix.apply(i, j))

    val predictRdd = whiteModel.transform(rddVector)

    val schema = Array.range(0, m).map(i => StructField("component_" + i, DoubleType))
    val newDataFrame = sqlc.createDataFrame(
      predictRdd.map(vec => Row.fromSeq(vec.toArray)), StructType(schema))

    newDataFrame.show()




  }
}
