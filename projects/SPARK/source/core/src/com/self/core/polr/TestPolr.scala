package com.self.core.polr

import com.self.core.baseApp.myAPP
import com.self.core.polr.models.{Polr, TestData, Utils}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * editor：Xuhao
  * date： 2017/5/4 10:00:00
  */

/**
  * 测试有序回归
  */

object TestPolr extends myAPP {
  def simulate(): Unit = {
    val testData = TestData.testDataMeetingPoint
    import TestData.ArrayToMatrix
    val lst = testData.toMatrixArrayByRow(500, 3)

    val rdd = sc.parallelize(lst).map(Row.fromSeq(_))
    val rawDataDF = sqlc.createDataFrame(rdd, StructType(Array(StructField("x1", DoubleType),
      StructField("x2", DoubleType), StructField("y", StringType))))

    outputrdd.put("rawDataDF", rawDataDF)
  }

  simulate()


  override def run(): Unit = {
    /**
      * 一些参数的处理
      */
    /** 0)获取基本的系统变量 */
    //    val jsonparam = "<#zzjzParam#>"
    //    val gson = new Gson()
    //    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    //    val parser = new JsonParser()
    //    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
    //    val z1 = z
    //    val rddTableName = "<#zzjzRddName#>"

    val rawDataDF = outputrdd.get("rawDataDF").asInstanceOf[DataFrame]
    rawDataDF.show()

    /** 2)因变量列名 */
    val (idColName: String, idColType: String) = ("y", "string")

    /** 3)获取分级信息 */
    val gradeFormat = "byScheduler" // categories


    /** 4)获取特征列 */
    val featureCols: ArrayBuffer[(String, String)] = ArrayBuffer(("x1", "double"), ("x2", "double"))


    // 确认特征列名均存在
    require((featureCols.map(_._1) :+ idColName).forall(s => rawDataDF.schema.fieldNames contains s), "您输入的列名信息有部分不存在")

    val (trainData: RDD[LabeledPoint], categories) = gradeFormat match {
      case "byScheduler" =>
        val scheduler = "meetingPoint" // test1 --meetingPoint
        scheduler match {
          case "byNumber" =>
            val rdd: RDD[(Double, DenseVector)] = Utils.getRddIdByDouble(rawDataDF: DataFrame,
              idColName: String,
              idColType: String,
              featureCols: ArrayBuffer[(String, String)])
            rdd.cache()
            Utils.transform(sc, rdd, featureCols.length)
          case "byLowerCaseAlpha" =>
            val categories = Array(
              "a", "b", "c", "d", "e", "f", "g",
              "h", "i", "j", "k", "l", "m", "n",
              "o", "p", "q", "r", "s", "t", "u",
              "v", "w", "x", "y", "z").zipWithIndex.map {
              case (key, index) => (key, index.toDouble)
            }.toMap
            val rdd: RDD[(String, DenseVector)] = Utils.getRddIdByString(rawDataDF: DataFrame,
              idColName: String,
              idColType: String,
              featureCols: ArrayBuffer[(String, String)])
            rdd.cache()
            Utils.transform(sc, rdd, categories, featureCols.length)
          case "byUpperCaseAlpha" =>
            val categories = Array(
              "a", "b", "c", "d", "e", "f", "g",
              "h", "i", "j", "k", "l", "m", "n",
              "o", "p", "q", "r", "s", "t", "u",
              "v", "w", "x", "y", "z").zipWithIndex.map {
              case (key, index) => (key.toUpperCase(), index.toDouble)
            }.toMap
            val rdd: RDD[(String, DenseVector)] = Utils.getRddIdByString(rawDataDF: DataFrame,
              idColName: String,
              idColType: String,
              featureCols: ArrayBuffer[(String, String)])
            rdd.cache()
            Utils.transform(sc, rdd, categories, featureCols.length)

          case "meetingPoint" =>
            val categories = Array("甲", "乙", "丙", "丁").reverse
              .zipWithIndex.map { case (key, index) => (key, index.toDouble) }.toMap
            val rdd: RDD[(String, DenseVector)] = Utils.getRddIdByString(rawDataDF: DataFrame,
              idColName: String,
              idColType: String,
              featureCols: ArrayBuffer[(String, String)])
            rdd.cache()
            Utils.transform(sc, rdd, categories, featureCols.length)

          case "abcd" =>
            val categories = Array("A", "B", "C", "D").reverse.zipWithIndex.map {
              case (key, index) => (key, index.toDouble)
            }.toMap
            val rdd: RDD[(String, DenseVector)] = Utils.getRddIdByString(rawDataDF: DataFrame,
              idColName: String,
              idColType: String,
              featureCols: ArrayBuffer[(String, String)])
            rdd.cache()
            Utils.transform(sc, rdd, categories, featureCols.length)
        }

      case "byHand" =>
        var categories: mutable.Map[String, Double] = scala.collection.mutable.Map.empty
        //        val categoryArray = gradeInfoObj.get("category").getAsJsonArray
        //        for (i <- 0 until categoryArray.size()) {
        //          val tup = (categoryArray.get(i).getAsJsonObject.get("categoryValue").getAsString.trim,
        //            i.toDouble)
        //          categories += tup
        //        }

        val rdd: RDD[(String, DenseVector)] = Utils.getRddIdByString(rawDataDF: DataFrame,
          idColName: String,
          idColType: String,
          featureCols: ArrayBuffer[(String, String)])
        rdd.cache()
        Utils.transform(sc, rdd, categories.toMap, featureCols.length)
    }

    if (trainData.count() <= 1 + featureCols.length + categories.size)
      throw new Exception("经过数据因子化（对因变量加入级别信息）后数据数量少于过度识别的数目。可能的原因是：" +
        "输入的分级信息和数据不一致导致很多数据找不到分级信息。")

    val resultModel = new Polr(categories.size, 20000, 0.5, 1.0).run(trainData)

    val categoriesArray = categories.toArray.sortBy(_._2).map(_._1.toString)

    val printArray = categories.toArray.sortBy(_._2).map(_._1).sliding(2).map(_.mkString(" | ")).toArray
    println("Coefficients:\r\n" + resultModel.weights.toArray.mkString("    "))
    println()
    println("Intercepts:")
    println(printArray.zip(resultModel.intercepts).map(s => s._1 + "    " + s._2).mkString(", \r\n"))


    val resultModelBC = rawDataDF.sqlContext.sparkContext.broadcast(resultModel).value
    val newRdd = trainData.map(labeledPoint =>
      (labeledPoint, resultModelBC.fit(labeledPoint.features)))
      .map { case (labeledPoint, index) => {
        labeledPoint.features.toDense.values :+ categoriesArray(labeledPoint.label.toInt) :+ categoriesArray(index)
      }
      }
    newRdd.map(x => Row.fromSeq(x))
    val featureSchema = featureCols.map(x => rawDataDF.schema.apply(x._1))
    val newDataDF = rawDataDF.sqlContext.createDataFrame(
      newRdd.map(x => Row.fromSeq(x)), StructType(featureSchema :+ StructField(idColName, StringType)
        :+ StructField(idColName + "_fit", StringType)))
    newDataDF.show()


  }
}
