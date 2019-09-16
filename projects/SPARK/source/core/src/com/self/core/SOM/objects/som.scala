package com.self.core.SOM.objects

import com.google.gson.{Gson, JsonParser}
import com.self.core.SOM.utils.Assembler
import com.self.core.baseApp.myAPP
import com.self.core.featurePretreatment.utils.Tools
import com.self.core.SOM.utils.Assembler
import com.self.core.featurePretreatment.utils.Tools
import org.apache.spark.SparkException
import org.apache.spark.mllib.clustering.{SOM, SOMGrid1Dims, SOMGrid2Dims, SOMModel}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * editor：Xuhao
  * date： 2018/7/17 10:00:00
  */

/**
  * SOM算法
  * ----
  * 自组织映射神经网络 --Self Organizing Map
  * 是一种无监督学习算法，最终的输出结果是带有拓扑结构的聚类，因此划归入聚类算法。
  * 参考文献： @see The element of statistical learning 14.4, 这里实现的是批处理模式
  * ----
  * 虽然叫做自组织神经网络但，由于其本质上是一种无监督学习的聚类算法，没有误差的反向传播等机制，
  * 算法本质上的迭代过程更接近于KMeans
  * ----
  * 改进脚本实现的是批处理版本：
  * 1）相对于online版本没有学习率递减和邻域递减
  * 2）终止条件是神经元(‘聚类’)中心不再发生变化
  * 3）邻域函数目前是高斯函数
  * 4）神经元之间的拓扑结构可以是一维或二维，二维结构可以是矩形或正六边形。
  * --拓扑结构的作用：拓扑结构决定了神经元之间的距离，从而决定邻域，决定了神经元的更新。
  */

object som extends myAPP {
  override def run(): Unit = {
    /** 0)一些系统变量设置 */
    val jsonparam = "<#zzjzParam#>"
    val rddTableName = "<#zzjzRddName#>"
    val gson = new Gson()
    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject

    val z1 = outputrdd

    /** 1)获取表名、处理类型、特征列名、输出列名 */
    val tableName = p.get("inputTableName").trim // 该表可能用于训练也可能用于预测
    val rawDataDF = z1.get(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]

    val featureColsArr = pJsonParser.get("featureCols").getAsJsonArray
    val featureCols = Array.range(0, featureColsArr.size()).map {
      idx =>
        val name = featureColsArr.get(idx).getAsJsonObject.get("name").getAsString
        Tools.columnExists(name, rawDataDF, true)
        name
    }

    val processTypeObj = pJsonParser.get("processType").getAsJsonObject
    val processType = processTypeObj.get("value").getAsString // 请选择处理方式 train || predict

    val outputCol = p.get("outputCol")
    require(outputCol.length < 100, "您输入的列名字符数不能超过100")
    val specialCharacter = Seq(
      '(', ')', '（', '）', '`', ''', '"', '。',
      ';', '.', ',', '%', '^', '$', '{', '}',
      '[', ']', '/', '\\', '+', '-', '*', '、',
      ':', '<', '>')
    val validCharacter = Seq('_', '#')
    specialCharacter.foreach {
      char =>
        require(!(outputCol contains char), s"您输入的列名${outputCol}中包含特殊字符：$char," +
          s" 列名信息中不能包含以下特殊字符：${specialCharacter.mkString(",")}," +
          s" 如果您需要用特殊字符标识递进关系，您可以使用一下特殊字符：${validCharacter.mkString(",")}")
    }
    require(!Tools.columnExists(outputCol, rawDataDF, false), "您输入的列名在数据有已经存在")


    /** 2)将获取的信息集成为一个RDD[Vector] */
    import org.apache.spark.sql.functions.col

    val numFeatures = try {
      val row = rawDataDF.head()
      val featuresArr = featureCols.map(name => row.get(row.fieldIndex(name)))

      try {
        Assembler.assemble(featuresArr: _*).size
      } catch {
        case e: Exception => throw new SparkException(s"在向量集成过程中失败，具体信息${e.getMessage}")
      }
    } catch {
      case e: Exception => throw new Exception(s"未能获得首条记录对应的特征长度，具体信息${e.getMessage}")
    }

    val data = rawDataDF.select(featureCols.map(col): _*).rdd.map {
      row =>
        val featuresArr = featureCols.map(name => row.get(row.fieldIndex(name)))

        val vec = try {
          Assembler.assemble(featuresArr: _*)
        } catch {
          case e: Exception => throw new SparkException(s"在向量集成过程中失败，具体信息${e.getMessage}")
        }

        require(vec.size == numFeatures, s"该算子要求所有的特征向量长度应该一致，但您所有的列集成为一个特征向量" +
          s"${vec}后, 长度为${vec.size}")
        vec
    }

    /** 3)数据处理过程 */

    val model: SOMModel = processType match {
      case "train" =>
        val somDim = processTypeObj.get("somDim").getAsJsonObject.get("value").getAsString
        val somGrid = somDim match {
          case "1" =>
            val grids = try {
              processTypeObj.get("somDim").getAsJsonObject.get("grids").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new SparkException(s"在拓扑结构构件中，获得神经元个数中失败，" +
                s"具体信息${e.getMessage}")
            }
            require(grids > 0 && grids <= 1000000, s"神经元个数应该小于10^6, 而一维网格数为$grids")
            new SOMGrid1Dims(grids)

          case "2" =>
            val topology = processTypeObj.get("somDim").getAsJsonObject.get("topology").getAsString

            val xGrid = try {
              processTypeObj.get("somDim").getAsJsonObject.get("xGrid").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new SparkException(s"在拓扑结构构件中，获得神经元个数中失败，" +
                s"具体信息${e.getMessage}")
            }
            val yGrid = try {
              processTypeObj.get("somDim").getAsJsonObject.get("yGrid").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new SparkException(s"在拓扑结构构件中，获得神经元个数中失败，" +
                s"具体信息${e.getMessage}")
            }

            require(xGrid * yGrid > 0 && xGrid * yGrid <= 1000000,
              s"神经元个数应该小于10^6，而二维乘积数为${xGrid * yGrid}")

            new SOMGrid2Dims(xGrid, yGrid, topology)
        }

        val maxIterations = try {
          processTypeObj.get("maxIterations").getAsString.toDouble.toInt
        } catch {
          case e: Exception => throw new SparkException(s"在拓扑结构构件中，获得神经元个数中失败，" +
            s"具体信息${e.getMessage}")
        }
        require(maxIterations >= 10, "您输入的迭代次数至少大于10，否则效果可能不理想")

        val seed = {
          val seedType = try {
            processTypeObj.get("seedObj")
              .getAsJsonObject.get("value").getAsString
          } catch {
            case e: Exception => throw new Exception(s"请选择随机数种子输入信息, ${e.getMessage}")
          }

          seedType match {
            case "true" =>
              val seedValue = try {
                processTypeObj.get("seedObj").getAsJsonObject
                  .get("seed").getAsString.toDouble.toLong
              } catch {
                case e: Exception => throw new SparkException(s"在获得随机数种子中失败，" +
                  s"具体信息${e.getMessage}")
              }
              Some(seedValue)

            case "false" =>
              None
          }
        }

        val sigma = try {
          processTypeObj.get("sigma").getAsString.toDouble
        } catch {
          case e: Exception => throw new SparkException(s"在获得sigma值时失败，" +
            s"具体信息${e.getMessage}")
        }
        require(sigma > 0.0, "您输入的sigma值需要大于0")

        val canopyString = util.Try {
          processTypeObj.get("canopy").getAsString
        }.getOrElse("1.0")

        val canopy = try {
          scala.math.max(canopyString.toDouble, 1.0) // 这个参数默认可以不设置
        } catch {
          case e: Exception => throw new SparkException(s"在将最大邻域转为数值类型时失败，" +
            s"具体信息${e.getMessage}")
        }

        val model = new SOM(somGrid, numFeatures, maxIterations, 1E-4, seed, sigma, canopy).run(data)

        if (processTypeObj.get("isPersist").getAsJsonObject.get("value").getAsString == "true") {
          val path = processTypeObj.get("isPersist").getAsJsonObject.get("path").getAsString
          require(!(path contains '\\'), "路径不能包含\\等特殊字符")
          model.save(rawDataDF.sqlContext, path)
          outputrdd.put(rddTableName + "_模型路径", path)
        }
        model

      case "predict" =>
        val getTypeObj = processTypeObj.get("getTypeObj").getAsJsonObject
        getTypeObj.get("value").getAsString match {
          case "fromMemory" =>
            val modelName = getTypeObj.get("modelName").getAsString
            z1.get(modelName).asInstanceOf[SOMModel]
          case "fromPersist" =>
            val path = getTypeObj.get("path").getAsString
            require(!(path contains '\\'), "路径不能包含\\等特殊字符")
            SOMModel.SaveLoadV1_0.load(rawDataDF.sqlContext, path)
        }
    }

    require(model.numFeatures == numFeatures,
      s"训练数据时的特征向量长度${model.numFeatures}和预测数据特征向量长度${numFeatures}不一致")

    val modelBC = rawDataDF.sqlContext.sparkContext.broadcast(model)

    val resultRdd = rawDataDF.rdd.map {
      row =>
        val featuresArr = featureCols.map(name => row.get(row.fieldIndex(name)))

        val vec = try {
          Assembler.assemble(featuresArr: _*)
        } catch {
          case e: Exception => throw new SparkException(s"在向量集成过程中失败，具体信息${e.getMessage}")
        }

        require(vec.size == numFeatures, s"该算子要求所有的特征向量长度应该一致，但您所有的列集成为一个特征向量" +
          s"${vec}后, 长度为${vec.size}")

        val value = modelBC.value.predictAxisVector(vec)
        Row.merge(row, Row(value))
    }

    val schema = rawDataDF.schema.add(StructField(outputCol, new VectorUDT))

    val newDataFrame = rawDataDF.sqlContext.createDataFrame(resultRdd, StructType(schema))

    /** 输出结果 */
    outputrdd.put(rddTableName, newDataFrame)
    newDataFrame.registerTempTable(rddTableName)
    newDataFrame.sqlContext.cacheTable(rddTableName)

    outputrdd.put(rddTableName + "_模型", model)

  }
}
