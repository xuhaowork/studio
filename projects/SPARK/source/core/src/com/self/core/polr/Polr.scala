package com.self.core.polr

import com.google.gson.{Gson, JsonParser}
import com.self.core.baseApp.myAPP
import com.self.core.polr.models.{Polr, Utils}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 有序回归算子
  */
/**
  * editor：Xuhao
  * date： 2017/5/4 10:00:00
  */

object Polr extends myAPP {
  override def run(): Unit = {
    /**
      * 一些参数的处理
      */
    /** 0)获取基本的系统变量 */
    val jsonparam = "<#zzjzParam#>"
    val gson = new Gson()
    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
    val z1 = memoryMap
    val rddTableName = "<#zzjzRddName#>"

    /** 1)获取DataFrame */
    val tableName = p.get("inputTableName").trim
    val rawDataDF = z1.get(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]

    /** 2)因变量列名 */
    val (idColName: String, idColType: String) = (pJsonParser.getAsJsonArray("idColName").get(0).getAsJsonObject.get("name").getAsString,
      pJsonParser.getAsJsonArray("idColName").get(0).getAsJsonObject.get("datatype").getAsString)

    /** 3)获取分级信息 */
    val gradeInfoObj = pJsonParser.getAsJsonObject("gradeInfoObj")
    val gradeFormat = gradeInfoObj.get("value").getAsString // byScheduler  categories


    /** 4)获取特征列 */
    val featureColsObj = pJsonParser.getAsJsonArray("featureColsObj")
    val featureCols: ArrayBuffer[(String, String)] = ArrayBuffer.empty[(String, String)]
    for (i <- 0 until featureColsObj.size()) {
      val tup = (featureColsObj.get(i).getAsJsonObject.get("name").getAsString,
        featureColsObj.get(i).getAsJsonObject.get("datatype").getAsString)
      featureCols += tup
    }

    // 确认特征列名均存在
    require((featureCols.map(_._1) :+ idColName).forall(s => rawDataDF.schema.fieldNames contains s), "您输入的列名信息有部分不存在")

    val (trainData: RDD[LabeledPoint], categories) = gradeFormat match {
      case "byScheduler" =>
        val schedulerObj = gradeInfoObj.get("scheduler").getAsJsonObject
        val scheduler = schedulerObj.get("value").getAsString
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
        val categoryArray = gradeInfoObj.get("category").getAsJsonArray
        for (i <- 0 until categoryArray.size()) {
          val tup = (categoryArray.get(i).getAsJsonObject.get("categoryValue").getAsString.trim,
            i.toDouble)
          categories += tup
        }
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

    trainData.repartition(5)
    trainData.cache()

    val resultModel = new Polr(categories.size, 200, 0.5, 1.0).run(trainData)

    val categoriesArray = categories.toArray.sortBy(_._2).map(_._1.toString)

    /** 打印系数信息 */
    val printArray = categories.toArray.sortBy(_._2).map(_._1).sliding(2).map(_.mkString(" | ")).toArray
    println("Coefficients:\r\n" + resultModel.weights.toArray.mkString("    "))
    println()
    println("Intercepts:")
    println(printArray.zip(resultModel.intercepts).map(s => s._1 + "    " + s._2).mkString(", \r\n"))

    /** 将列名按类型再转回去，尽量保持类型和原来一致 */
    val resultModelBC = rawDataDF.sqlContext.sparkContext.broadcast(resultModel).value
    val newRdd = trainData.map(labeledPoint =>
      (labeledPoint, resultModelBC.fit(labeledPoint.features))).map {
      case (labeledPoint, index) =>
        labeledPoint.features.toDense.values :+ categoriesArray(labeledPoint.label.toInt) :+ categoriesArray(index)
    }
    newRdd.map(x => Row.fromSeq(x))
    val featureSchema = featureCols.map(x => rawDataDF.schema.apply(x._1))
    val doubleTypeSchema = featureSchema.map(field => StructField(field.name, DoubleType))
    var newDataDF = rawDataDF.sqlContext.createDataFrame(
      newRdd.map(x => Row.fromSeq(x)), StructType(doubleTypeSchema :+ StructField(idColName, StringType)
        :+ StructField(idColName + "_fit", StringType)))

    for (each <- featureSchema) {
      newDataDF = try {
        newDataDF.withColumn(each.name, col(each.name).cast(each.dataType))
      } catch {
        case _: Exception =>
          println(s"${each.name}不能转为原有类型${each.dataType.toString}, 算子按Double类型给您存储。")
          newDataDF
      }
    }

    newDataDF = try {
      newDataDF.withColumn(idColName, col(idColName).cast(idColType))
        .withColumn(idColName + "_fit", col(idColName + "_fit").cast(idColType))
    } catch {
      case _: Exception =>
        println(s"${idColName}不能转为原有类型$idColType, 算子按String类型给您存储。")
        newDataDF
    }


    /** output */
    newDataDF.show()
    newDataDF.registerTempTable(rddTableName)
    newDataDF.sqlContext.cacheTable(rddTableName)
    outputrdd.put("<#zzjzRddName#>", newDataDF)


  }

}
