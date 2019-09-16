package com.self.core.featurePretreatment.objects

import com.google.gson.{Gson, JsonParser}
import com.self.core.baseApp.myAPP
import com.self.core.featurePretreatment.utils.Tools
import com.self.core.featurePretreatment.models._
import com.self.core.featurePretreatment.utils.Tools
import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, VectorUDT}
import org.apache.spark.sql.columnUtils.DataTypeImpl._
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

/**
  * editor: xuhao
  * date: 2018-06-20 08:30:00
  */

/**
  * 特征预处理
  * ----
  *
  * 特征转换的算法分类为：“属性类特征变换”、“数值类特征变换”、“数量尺度变换”、“字符串和数值索引互转”、“特征选择”五种
  * 1.属性类特征变换 ——这里“属性类特征”指的是如词汇等这些没有数值方面大小或顺序的特征
  * 正则表达式分词；
  * 词频统计；
  * hash词频统计；
  * 文本向量化；
  * 停用词移除；
  * n-gram
  * 2.数值类特征变换
  * 离散化；
  * 独热编码；
  * 逆文档频率转换；
  * 低变异性数值特征索引化；
  * 提取主成分；
  * 多项式分解；
  * 离散余弦变换
  * 3.数量尺度变换——也是数值类型的特征的转换，不过只是更加简单的尺度变换
  * 正则化；
  * 标准化；
  * 加权
  * 4.字符串和数值索引互转
  * 属性类的特征转为数值索引；
  * 数值索引转为对应字符串
  * 5.特征选择——多个特征之间的简单拆分或组合，称为特征选择，区别于特征转换，特征选择往往不涉及数值的变化。
  * 选取子向量；
  * 卡方特征选择；
  * 向量集成；
  * 向量拆分；
  * 数组集成；
  * 一层数组拆分；
  */

object FeaturePretreatment extends myAPP {

  override def run(): Unit = {
    def transform2DenseVector(colName: String,
                              data: DataFrame): (DataFrame, String) = {
      import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
      import org.apache.spark.sql.NullableFunctions
      import org.apache.spark.sql.functions.col
      Tools.columnExists(colName, data, true)
      data.schema(colName).dataType in(Seq("vector"), true)

      val transformUDF = NullableFunctions.udf((v: Vector) =>
        v match {
          case sv: SparseVector => sv.toDense
          case dv: DenseVector => dv
          case _ => throw new Exception("没有识别到vector类型")
        })
      val newName = confirmValidname(colName + "_denseVector", data)

      (data.withColumn(newName, transformUDF(col(colName))), newName)
    }

    def confirmValidname(colName: String, data: DataFrame, suffix: String = "_suffix"): String = {
      val names = data.schema.fieldNames.toSet
      var nameWithSuffix = colName
      while (names(colName)) {
        nameWithSuffix += suffix
      }
      nameWithSuffix
    }

    def confirmIndexValue(colName: String, data: DataFrame): Unit = {
      Tools.columnExists(colName, data, true)
      data.schema(colName).dataType in(Seq("numeric"), true)
      val notIndexDF = data.select(colName).filter(s"$colName != floor($colName) or $colName < 0")
      val invalidValue = notIndexDF.take(10).map(_.get(0).toString).mkString(",")
      require(notIndexDF.count() <= 0,
        s"您输入的数据有不是索引类型的数据，您可以通过'记录选择'或'SQL'组件过滤掉这些值。部分数据如下：$invalidValue")
    }


    /**
      * 一些参数的处理
      */
    /** 0)获取基本的系统变量 */
    val jsonparam = "<#zzjzParam#>"
    val gson = new Gson()
    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    val z1 = outputrdd


    /** 1)获取DataFrame */
    val tableName = p.get("inputTableName").trim
    val rawDataFrame = z1.get(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject

    val pretreatmentObj = pJsonParser.getAsJsonObject("pretreatmentType")
    val pretreatmentType = pretreatmentObj.get("value").getAsString
    val newDataFrame = pretreatmentType match {
      case "attributesDataTransform" => // 属性类特征转换  一个输入一个输出
        val inputCol = pretreatmentObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
        val outputCol = pretreatmentObj.get("outputCol").getAsString

        val pretreatObj = pretreatmentObj.get("pretreatment").getAsJsonObject
        pretreatObj.get("value").getAsString match {
          case "TokenizerByRegex" => // 1.1.	正则表达式分词 string => array<string>
            val pattern = pretreatObj.get("pattern").getAsString

            val gaps = try {
              pretreatObj.get("gaps").getAsString == "true"
            } catch {
              case _: Exception => throw new Exception("是否转为小写，参数输入出现异常")
            }

            val minTokenLength = try {
              pretreatObj.get("minTokenLength").getAsString.toDouble.toInt
            } catch {
              case _: Exception => throw new Exception("您输入的最小分词数不能转为Int类型")
            }
            require(minTokenLength >= 1, "您输入的最小分词数需要为正整数")

            Tools.columnExists(inputCol, rawDataFrame, true)
            rawDataFrame.schema(inputCol).dataType in(Seq("string"), true)
            //            Tools.columnTypesIn(inputCol, rawDataFrame, Array("string"), true)
            new TokenizerByRegex(rawDataFrame)
              .setParams(TokenizerByRegexParamsName.inputCol, inputCol)
              .setParams(TokenizerByRegexParamsName.outputCol, outputCol)
              .setParams(TokenizerByRegexParamsName.gaps, gaps)
              .setParams(TokenizerByRegexParamsName.minTokenLength, minTokenLength)
              .setParams(TokenizerByRegexParamsName.pattern, pattern)
              .run()
              .data

          case "CountWordVector" => // 1.2. 词频统计 array<string> => vector
            val vocabSizeString = try {
              pretreatObj.get("vocabSize").getAsString.trim
            } catch {
              case e: Exception => throw new Exception(s"没有找到词汇数参数有误，具体错误为：${e.getMessage}")
            }

            val vocabSize = if (vocabSizeString contains '^') {
              try {
                vocabSizeString.split('^').map(_.trim.toDouble).reduceLeft((d, s) => scala.math.pow(d, s)).toInt
              } catch {
                case e: Exception => throw new Exception(s"您输入词汇数参数表达式中包含指数运算符^，" +
                  s"但^两侧可能包含不能转为数值类型的字符，或您输入的指数过大超过了2^31，具体错误为.${e.getMessage}")
              }
            } else {
              try {
                vocabSizeString.toDouble.toInt
              } catch {
                case e: Exception => throw new Exception(s"输入的词汇数参数不能转为数值类型，具体错误为.${e.getMessage}")
              }
            }
            require(vocabSize > 0, "词汇数需要为大于0的正整数")

            val minTf = try {
              pretreatObj.get("minTf").getAsString.toDouble
            } catch {
              case e: Exception => throw new Exception(s"您输入词频参数有误，请输入数值类型," +
                s" 具体错误为：${e.getMessage}")
            }

            require(minTf >= 0, "最低词频需要大于等于0")

            val minDf = try {
              pretreatObj.get("minDf").getAsString.toDouble
            } catch {
              case e: Exception => throw new Exception(s"您输入的最低文档频率有误，请输入数值类型," +
                s" 具体错误为：${e.getMessage}")
            }

            require(minDf >= 0, "最低文档频率需要大于等于0")

            Tools.columnTypesIn(inputCol, rawDataFrame, true, ArrayType(StringType, true), ArrayType(StringType, false))
            new CountWordVector(rawDataFrame)
              .setParams(CountWordVectorParamsName.inputCol, inputCol)
              .setParams(CountWordVectorParamsName.outputCol, outputCol)
              .setParams(CountWordVectorParamsName.vocabSize, vocabSize)
              .setParams(CountWordVectorParamsName.minTf, minTf)
              .setParams(CountWordVectorParamsName.minDf, minDf)
              .run()
              .data

          case "HashTF" => // 1.3. Hash词频统计
            val numFeatureString = try {
              pretreatObj.get("numFeature").getAsString.trim
            } catch {
              case e: Exception => throw new Exception(s"没有找到词汇数参数有误，具体错误为：${e.getMessage}")
            }

            val numFeature = if (numFeatureString contains '^') {
              try {
                numFeatureString.split('^').map(_.trim.toDouble).reduceLeft((d, s) => scala.math.pow(d, s)).toInt
              } catch {
                case e: Exception => throw new Exception(s"您输入词汇数参数表达式中包含指数运算符^，" +
                  s"但^两侧可能包含不能转为数值类型的字符，或您输入的指数过大超过了2^31，具体错误为.${e.getMessage}")
              }
            } else {
              try {
                numFeatureString.toDouble.toInt
              } catch {
                case e: Exception => throw new Exception(s"输入的词汇数参数不能转为数值类型，具体错误为.${e.getMessage}")
              }
            }

            require(numFeature > 0, "hash特征数需要为大于0的整数")

            Tools.columnTypesIn(inputCol, rawDataFrame, true, ArrayType(StringType, true), ArrayType(StringType, false))
            new HashTF(rawDataFrame)
              .setParams(HashTFParamsName.numFeatures, numFeature)
              .setParams(HashTFParamsName.inputCol, inputCol)
              .setParams(HashTFParamsName.outputCol, outputCol)
              .run()
              .data

          case "WordToVector" => // 1.4. 文档向量化  array<string> => vector
            val vocabSizeString = try {
              pretreatObj.get("vocabSize").getAsString.trim
            } catch {
              case e: Exception => throw new Exception(s"没有找到向量长度，具体错误为：${e.getMessage}")
            }

            val vocabSize = if (vocabSizeString contains '^') {
              try {
                vocabSizeString.split('^').map(_.trim.toDouble).reduceLeft((d, s) => scala.math.pow(d, s)).toInt
              } catch {
                case e: Exception => throw new Exception(s"您输入向量长度参数表达式中包含指数运算符^，" +
                  s"但^两侧可能包含不能转为数值类型的字符，或您输入的指数过大超过了2^31，具体错误为.${e.getMessage}")
              }
            } else {
              try {
                vocabSizeString.toDouble.toInt
              } catch {
                case e: Exception => throw new Exception(s"输入的词汇数参数不能转为数值类型，具体错误为.${e.getMessage}")
              }
            }
            require(vocabSize > 0, "词汇数需要大于0")

            val minCount = try {
              pretreatObj.get("minCount").getAsString.trim.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"参数最小词频有误，具体错误为：${e.getMessage}")
            }
            require(minCount > 0, "最小词频需要大于0")

            val stepSize = try {
              pretreatObj.get("stepSize").getAsString.trim.toDouble
            } catch {
              case e: Exception => throw new Exception(s"参数学习率有误，具体错误为：${e.getMessage}")
            }
            require(stepSize > 0, "学习率需要大于0")

            val numPartitions = try {
              pretreatObj.get("numPartitions").getAsString.trim.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"没有找到词汇数参数有误，具体错误为：${e.getMessage}")
            }
            require(numPartitions > 0, "并行度需要为正整数")

            val numIterations = try {
              pretreatObj.get("numIterations").getAsString.trim.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"没有找到词汇数参数有误，具体错误为：${e.getMessage}")
            }
            require(numIterations > 0, "模型迭代数需要为正整数")

            val rdSeed = new java.util.Random().nextLong()
            val seed = util.Try {
              pretreatObj.get("seed").getAsString.trim.toDouble.toLong
            }.getOrElse(rdSeed)

            Tools.columnTypesIn(inputCol, rawDataFrame, true, ArrayType(StringType, true), ArrayType(StringType, false))
            new WordToVector(rawDataFrame)
              .setParams(WordToVectorParamsName.inputCol, inputCol)
              .setParams(WordToVectorParamsName.outputCol, outputCol)
              .setParams(WordToVectorParamsName.vocabSize, vocabSize)
              .setParams(WordToVectorParamsName.stepSize, stepSize)
              .setParams(WordToVectorParamsName.minCount, minCount)
              .setParams(WordToVectorParamsName.seed, seed)
              .setParams(WordToVectorParamsName.numPartitions, numPartitions)
              .setParams(WordToVectorParamsName.numIterations, numIterations)
              .run()
              .data

          case "StopWordsRemover" => // 1.5 停用词移除  array<string> => array<string>
            val stopWordsFormat = pretreatObj.get("stopWordsFormat").getAsJsonObject.get("value").getAsString

            val stopWords = stopWordsFormat match {
              case "English" => StopWordsUtils.English
              case "Chinese" => StopWordsUtils.chinese
              case "byHand" =>
                val stopWordsArr = pretreatObj.get("stopWordsFormat").getAsJsonObject.get("stopWords").getAsJsonArray
                Array.range(0, stopWordsArr.size())
                  .map(i => stopWordsArr.get(i).getAsJsonObject.get("word").getAsString)
              case "byFile" =>
                try {
                  val path = pretreatObj.get("stopWordsFormat").getAsJsonObject.get("path").getAsString
                  val separator = pretreatObj.get("stopWordsFormat").getAsJsonObject.get("separator").getAsString
                  sc.textFile(path).collect().flatMap(_.split(separator).map(_.trim))
                } catch {
                  case e: Exception => throw new Exception(s"读取分词文件失败，具体信息${e.getMessage}")
                }
            }

            val caseSensitive = pretreatObj.get("caseSensitive").getAsString == "true"

            Tools.columnTypesIn(inputCol, rawDataFrame, true, ArrayType(StringType, true), ArrayType(StringType, false))
            new StopWordsRmv(rawDataFrame)
              .setParams(StopWordsRemoverParamsName.inputCol, inputCol)
              .setParams(StopWordsRemoverParamsName.outputCol, outputCol)
              .setParams(StopWordsRemoverParamsName.caseSensitive, caseSensitive)
              .setParams(StopWordsRemoverParamsName.stopWords, stopWords)
              .run()
              .data

          case "NGram" => // 1.6 n-gram  array<string> => array<string>
            val n = try {
              pretreatObj.get("n").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的n值有误，请输入正整数值. ${e.getMessage}")
            }
            require(n > 0, "n值需要为正整数")

            Tools.columnTypesIn(inputCol, rawDataFrame, true, ArrayType(StringType, true), ArrayType(StringType, false))
            new NGramMD(rawDataFrame)
              .setParams(NGramParamsName.inputCol, inputCol)
              .setParams(NGramParamsName.outputCol, outputCol)
              .setParams(NGramParamsName.n, n)
              .run()
              .data
        }

      case "numericDataTransform" => // 数值类特征转换 一个输入一个输出
        val inputCol = pretreatmentObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
        val outputCol = pretreatmentObj.get("outputCol").getAsString

        val pretreatObj = pretreatmentObj.get("pretreatment").getAsJsonObject
        pretreatObj.get("value").getAsString match {
          case "Discretizer" => // 2.1 数值类型特征分箱 double => double
            Tools.columnTypesIn(inputCol, rawDataFrame, true, DoubleType)

            val binningFormat = pretreatObj.get("binningFormat").getAsJsonObject.get("value").getAsString
            binningFormat match {
              case "byWidth" =>
                val phase = try {
                  pretreatObj.get("binningFormat").getAsJsonObject.get("phase").getAsString.toDouble
                } catch {
                  case e: Exception => throw new Exception(s"您输入的分箱起始点信息有误, ${e.getMessage}")
                }
                val width = try {
                  pretreatObj.get("binningFormat").getAsJsonObject.get("width").getAsString.toDouble
                } catch {
                  case e: Exception => throw new Exception(s"您输入的窗宽信息有误, ${e.getMessage}")
                }

                new Discretizer(rawDataFrame)
                  .setParams(DiscretizerParams.inputCol, inputCol)
                  .setParams(DiscretizerParams.outputCol, outputCol)
                  .setParams(DiscretizerParams.discretizeFormat, "byWidth")
                  .setParams(DiscretizerParams.phase, phase)
                  .setParams(DiscretizerParams.width, width)
                  .run()
                  .data

              case "selfDefined" =>
                val arr = pretreatObj.get("binningFormat").getAsJsonObject.get("buckets").getAsJsonArray
                val buckets = try {
                  Array(0, arr.size()).map(i => arr.get(i).getAsJsonObject.get("bucket").getAsString.toDouble)
                } catch {
                  case e: Exception => throw new Exception(s"您输入的分隔信息有误, ${e.getMessage}")
                }

                val bucketsAddInfinity = try {
                  pretreatObj.get("binningFormat").getAsJsonObject.get("bucketsAddInfinity").getAsString == "true"
                } catch {
                  case e: Exception => throw new Exception(s"您输入的分隔信息有误, ${e.getMessage}")
                }

                try {
                  new Discretizer(rawDataFrame)
                    .setParams(DiscretizerParams.inputCol, inputCol)
                    .setParams(DiscretizerParams.outputCol, outputCol)
                    .setParams(DiscretizerParams.discretizeFormat, "selfDefined")
                    .setParams(DiscretizerParams.buckets, buckets)
                    .setParams(DiscretizerParams.bucketsAddInfinity, bucketsAddInfinity)
                    .run()
                    .data
                } catch {
                  case e: Exception if Array("Feature value", "out of Bucketizer bounds",
                    "Check your features, or loosen the lower/upper bound constraints.").forall(
                    s => e.getMessage contains s) =>
                    throw new Exception(s"有数据可能超出了边界，请您查看具体信息: ${e.getMessage}")
                  case error: Exception => throw error
                }
            }

          case "OneHotCoder" => // 2.2 独热编码  double => vector
            Tools.columnTypesIn(inputCol, rawDataFrame, true, DoubleType)
            val dropLast = pretreatObj.get("dropLast").getAsString == "true"
            new OneHotCoder(rawDataFrame)
              .setParams(OneHotCoderParams.inputCol, inputCol)
              .setParams(OneHotCoderParams.outputCol, outputCol)
              .setParams(OneHotCoderParams.dropLast, dropLast)
              .run()
              .data

          case "IDFTransformer" => // 2.3.	IDF转换  vector => vector
            Tools.columnTypesIn(inputCol, rawDataFrame, true, new VectorUDT)

            val minDocFreq = try {
              pretreatObj.get("minDocFreq").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的最小文档频率信息有误, ${e.getMessage}")
            }

            val requireVectorSameSize = util.Try(
              pretreatObj.get("requireVectorSameSize").getAsString.trim).getOrElse("") == "true"

            if (requireVectorSameSize)
              Tools.requireVectorSameSize(inputCol, rawDataFrame) // 判定向量长度一致

            new IDFTransformer(rawDataFrame)
              .setParams(IDFTransformerParams.inputCol, inputCol)
              .setParams(IDFTransformerParams.outputCol, outputCol)
              .setParams(IDFTransformerParams.minDocFreq, minDocFreq) // @todo: 必须为Int
              .run()
              .data

          case "Vector2Indexer" => // 2.4.	低变异性数值特征索引化  vector => vector
            val maxCategories = try {
              pretreatObj.get("maxCategories").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的最少不同值数, ${e.getMessage}")
            }

            val requireVectorSameSize = util.Try(
              pretreatObj.get("requireVectorSameSize").getAsString.trim).getOrElse("") == "true"

            if (requireVectorSameSize)
              Tools.requireVectorSameSize(inputCol, rawDataFrame) // 判定向量长度一致

            Tools.columnTypesIn(inputCol, rawDataFrame, true, new VectorUDT)
            new VectorIndexerTransformer(rawDataFrame)
              .setParams(VectorIndexerParams.inputCol, inputCol)
              .setParams(VectorIndexerParams.outputCol, outputCol)
              .setParams(VectorIndexerParams.maxCategories, maxCategories) // @todo
              .run()
              .data

          case "PCATransformer" => // 2.5.	主成分分析 vector => vector
            val componentNum = try {
              pretreatObj.get("p").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的主成分数, ${e.getMessage}")
            }

            Tools.columnTypesIn(inputCol, rawDataFrame, true, new VectorUDT) // 判定向量类型
          val requireVectorSameSize = util.Try(
            pretreatObj.get("requireVectorSameSize").getAsString.trim).getOrElse("") == "true"

            if (requireVectorSameSize)
              Tools.requireVectorSameSize(inputCol, rawDataFrame) // 判定向量长度一致

            new PCATransformer(rawDataFrame)
              .setParams(PCAParams.inputCol, inputCol)
              .setParams(PCAParams.outputCol, outputCol)
              .setParams(PCAParams.p, componentNum) // 需要小于向量长度
              .run()
              .data

          case "PlynExpansionTransformer" => // 2.6.	多项式展开  vector => vector
            val degree = try {
              pretreatObj.get("degree").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的展开式的幂, ${e.getMessage}")
            }

            Tools.columnTypesIn(inputCol, rawDataFrame, true, new VectorUDT)

            new PlynExpansionTransformer(rawDataFrame)
              .setParams(PlynExpansionParams.inputCol, inputCol)
              .setParams(PlynExpansionParams.outputCol, outputCol)
              .setParams(PlynExpansionParams.degree, degree)
              .run()
              .data

          case "DCTTransformer" =>
            val inverse = try {
              pretreatObj.get("inverse").getAsString.trim == "true"
            } catch {
              case e: Exception => throw new Exception(s"您输入的展开式的幂, ${e.getMessage}")
            }

            Tools.columnTypesIn(inputCol, rawDataFrame, true, new VectorUDT)
            new DCTTransformer(rawDataFrame)
              .setParams(DCTParams.inputCol, inputCol)
              .setParams(DCTParams.outputCol, outputCol)
              .setParams(DCTParams.inverse, inverse)
              .run()
              .data
        }

      case "numericScale" => // 数量尺度变换
        val inputCol = pretreatmentObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
        val outputCol = pretreatmentObj.get("outputCol").getAsString

        val pretreatObj = pretreatmentObj.get("pretreatment").getAsJsonObject

        // 判定向量类型
        Tools.columnTypesIn(inputCol, rawDataFrame, true, new VectorUDT)

        pretreatObj.get("value").getAsString match {
          case "NormalizerTransformer" => // 按行正则化 等长vector => vector
            val dimString = try {
              pretreatObj.get("p").getAsString.trim
            } catch {
              case e: Exception => throw new Exception(s"没有找到正则化信息，具体错误为：${e.getMessage}")
            }

            val dim = if (dimString contains '^') {
              try {
                dimString.split('^').map(_.trim.toDouble).reduceLeft((d, s) => scala.math.pow(d, s))
              } catch {
                case e: Exception => throw new Exception(s"您输入的正则化次数中包含指数运算符^，" +
                  s"但^两侧可能包含不能转为数值类型的字符，或您输入的指数过大超过了2^31，具体错误为.${e.getMessage}")
              }
            } else {
              try {
                dimString.toDouble.toInt
              } catch {
                case e: Exception => throw new Exception(s"输入的正则化次数不能转为数值类型，具体错误为.${e.getMessage}")
              }
            }
            require(dim > 0, "正则化次数需要大于0")

            new NormalizerTransformer(rawDataFrame)
              .setParams(NormalizerParams.inputCol, inputCol)
              .setParams(NormalizerParams.outputCol, outputCol)
              .setParams(NormalizerParams.p, dim)
              .run()
              .data

          case "scale" =>
            val scaleFormatObj = pretreatObj.get("scaleFormat").getAsJsonObject

            val (transformDF, denseVectorName) = transform2DenseVector(inputCol, rawDataFrame)

            // 判定向量长度一致
            val requireVectorSameSize = util.Try(
              pretreatObj.get("requireVectorSameSize").getAsString).getOrElse("") == "true"
            if (requireVectorSameSize)
              Tools.requireVectorSameSize(denseVectorName, transformDF)

            scaleFormatObj.get("value").getAsString match {
              case "StandardScaleTransformer" => // 等长vector => vector
                val withMean = scaleFormatObj.get("withMean").getAsString == "true"
                val withStd = scaleFormatObj.get("withStd").getAsString == "true"

                new StandardScaleTransformer(transformDF)
                  .setParams(StandardScaleParam.inputCol, denseVectorName)
                  .setParams(StandardScaleParam.outputCol, outputCol)
                  .setParams(StandardScaleParam.withStd, withStd)
                  .setParams(StandardScaleParam.withMean, withMean)
                  .run()
                  .data
                  .drop(denseVectorName)

              case "MinMaxScaleTransformer" => // 等长vector => vector
                val min = try {
                  scaleFormatObj.get("min").getAsString.trim.toDouble
                } catch {
                  case e: Exception => throw new Exception(s"请输入映射区间的最小值参数信息有误，${e.getMessage}")
                }
                val max = try {
                  scaleFormatObj.get("max").getAsString.trim.toDouble
                } catch {
                  case e: Exception => throw new Exception(s"请输入映射区间的最小值参数信息有误，${e.getMessage}")
                }

                new MinMaxScaleTransformer(transformDF)
                  .setParams(MinMaxScaleParam.inputCol, denseVectorName)
                  .setParams(MinMaxScaleParam.outputCol, outputCol)
                  .setParams(MinMaxScaleParam.min, min)
                  .setParams(MinMaxScaleParam.max, max)
                  .run()
                  .data
                  .drop(denseVectorName)
            }

          case "ElementProduct" => // 等长vector => vector
            val vectorSize = rawDataFrame.select(inputCol).head.getAs[Vector](0).size

            // 判定向量长度一致
            val requireVectorSameSize = util.Try(
              pretreatObj.get("requireVectorSameSize").getAsString).getOrElse("") == "true"
            if (requireVectorSameSize)
              Tools.requireVectorSameSize(inputCol, rawDataFrame)

            val transformingArray = pretreatObj.get("weights").getAsJsonArray
            val weights = Array.range(0, transformingArray.size())
              .map(i => transformingArray.get(i).getAsJsonObject.get("weight").getAsString.toDouble)

            require(vectorSize == weights.length, "您输入的权重需要和向量长度保持一致")

            new ElementProduct(rawDataFrame)
              .setParams(ElementProductParams.inputCol, inputCol)
              .setParams(ElementProductParams.outputCol, outputCol)
              .setParams(ElementProductParams.weight, weights)
              .run()
              .data
        }

      case "attributesWithNumeric" => // 数值类型和属性类型互转
        val inputCol = pretreatmentObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
        val outputCol = pretreatmentObj.get("outputCol").getAsString

        val pretreatObj = pretreatmentObj.get("pretreatment").getAsJsonObject
        pretreatObj.get("value").getAsString match {
          case "StringIndexTransformer" => // string => double
            // 判定向量类型
            Tools.columnTypesIn(inputCol, rawDataFrame, true, StringType)

            new StringIndexTransformer(rawDataFrame)
              .setParams(StringIndexParams.inputCol, inputCol)
              .setParams(StringIndexParams.outputCol, outputCol)
              .run()
              .data

          case "IndexerStringTransformer" =>
            // 判定向量类型
            Tools.columnTypesIn(inputCol, rawDataFrame, true, DoubleType, IntegerType, FloatType, LongType)

            val labelsArr = pretreatObj.get("labels").getAsJsonArray

            val requireIndexValue = try {
              pretreatObj.get("requireIndexValue").getAsString == "true"
            } catch {
              case e: Exception => throw new Exception(s"您输入的是否确认为索引类型的数值有误, ${e.getMessage}")
            }

            if (requireIndexValue)
              confirmIndexValue(inputCol, rawDataFrame)

            val labels = try {
              Array.range(0, labelsArr.size())
                .map(i => labelsArr.get(i).getAsJsonObject.get("label").getAsString)
            } catch {
              case e: Exception => throw new Exception(s"您输入的标签信息有误, ${e.getMessage}")
            }

            new IndexerStringTransformer(rawDataFrame)
              .setParams(IndexToStringParams.inputCol, inputCol)
              .setParams(IndexToStringParams.outputCol, outputCol)
              .setParams(IndexToStringParams.labels, labels)
              .run()
              .data
        }


      case "featureSelect" => // 特征选择, 可能多列输入一列输出
        val pretreatObj = pretreatmentObj.get("pretreatment").getAsJsonObject
        pretreatObj.get("value").getAsString match {
          case "VectorIndices" =>
            val inputCol = pretreatObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
            val outputCol = pretreatObj.get("outputCol").getAsString
            val indicesArr = pretreatObj.get("indices").getAsJsonArray
            val indices = Array.range(0, indicesArr.size())
              .map(i => indicesArr.get(i).getAsJsonObject.get("indice").getAsString.toDouble.toInt)

            // 判定向量类型
            Tools.columnTypesIn(inputCol, rawDataFrame, true, new VectorUDT)

            // 判定数组没有越界
            val vectorSize = rawDataFrame.select(inputCol).head match {
              case Row(v: Vector) => v.size
              case _ => throw new Exception(s"$inputCol 类型匹配不上vector类型")
            }

            require(!indices.isEmpty, "索引id不能为空")
            val filterArr = indices.filter(_ > vectorSize - 1)
            require(filterArr.isEmpty, s"您输入的索引id${filterArr.mkString(",")}超过了向量的索引范围")

            // 判定向量长度一致
            val requireVectorSameSize = util.Try(
              pretreatObj.get("requireVectorSameSize").getAsString).getOrElse("") == "true"
            if (requireVectorSameSize)
              Tools.requireVectorSameSize(inputCol, rawDataFrame)

            new VectorIndices(rawDataFrame)
              .setParams(VectorIndicesParams.inputCol, inputCol)
              .setParams(VectorIndicesParams.outputCol, outputCol)
              .setParams(VectorIndicesParams.indices, indices)
              .run()
              .data

          case "ChiFeatureSqSelector" =>
            val inputCol = pretreatObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
            val labeledCol = pretreatObj.get("labeledCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
            val outputCol = pretreatObj.get("outputCol").getAsString

            val topFeatureNums = try {
              pretreatObj.get("topFeatureNums").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的特征数有误，${e.getMessage}")
            }

            require(topFeatureNums > 0, "特征数需要为正整数")

            // 判定向量类型
            Tools.columnTypesIn(inputCol, rawDataFrame, true, new VectorUDT)
            // 判定向量长度一致
            val requireVectorSameSize = util.Try(
              pretreatObj.get("requireVectorSameSize").getAsString).getOrElse("") == "true"
            if (requireVectorSameSize)
              Tools.requireVectorSameSize(inputCol, rawDataFrame)

            new ChiFeatureSqSelector(rawDataFrame)
              .setParams(ChiFeatureSqSelectorParams.inputCol, inputCol)
              .setParams(ChiFeatureSqSelectorParams.outputCol, outputCol)
              .setParams(ChiFeatureSqSelectorParams.labeledCol, labeledCol)
              .setParams(ChiFeatureSqSelectorParams.topFeatureNums, topFeatureNums)
              .run()
              .data

          case "VectorAssembleTransformer" =>
            val inputColsArr = pretreatObj.get("inputCol").getAsJsonArray
            val inputCols = Array.range(0, inputColsArr.size())
              .map(i => inputColsArr.get(i).getAsJsonObject.get("name").getAsString)
            val outputCol = pretreatObj.get("outputCol").getAsString

            new VectorAssembleTransformer(rawDataFrame)
              .setParams(VectorAssembleParams.inputCol, inputCols)
              .setParams(VectorAssembleParams.outputCol, outputCol)
              .run()
              .data

          case "arrayAssemble" =>
            val inputColsArr = pretreatObj.get("inputCol").getAsJsonArray
            val inputCols = Array.range(0, inputColsArr.size())
              .map(i => inputColsArr.get(i).getAsJsonObject.get("name").getAsString)
            val outputCol = pretreatObj.get("outputCol").getAsString
            val elementTypeName = pretreatObj.get("elementTypeName").getAsString

            val assembleFunc = Tools.getTheUdfByType(elementTypeName)
            rawDataFrame.select(col("*"), assembleFunc(struct(inputCols.map(col): _*)).as(outputCol))

          case "vectorSplit" =>
            val inputCol = pretreatObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
            val outputColObj = pretreatObj.get("outputColFormat").getAsJsonObject
            val outputColFormat = outputColObj.get("value").getAsString
            val colNames = outputColFormat match {
              case "true" =>
                val outputColArr = outputColObj.get("outputColArr").getAsJsonArray
                require(outputColArr.size() > 0, "您输入的列名的长度至少为1")
                Some(Array.range(0, outputColArr.size())
                  .map(i => outputColArr.get(i).getAsJsonObject.get("name").getAsString))
              case "false" => None
            }

            val rawSchema = rawDataFrame.schema

            val elementType: DataType = rawSchema(inputCol).dataType match {
              case dt: ArrayType =>
                require(dt.elementType == StringType || dt.elementType == DoubleType,
                  s"数组需要为一层嵌套数组，并且其中元素类型" + s"只能是string或double，而您的类型" +
                    s"为${dt.elementType.simpleString}")
                dt.elementType
              case _: VectorUDT => new VectorUDT
              case others => throw new Exception(s"您输入列${inputCol}的类型为${others.simpleString}，不是array类型或double类型")
            }

            val inputID = rawSchema.fieldIndex(inputCol)
            val (size, fields) = if (colNames.isEmpty) {
              val size = rawDataFrame.select(inputCol).head.get(0) match {
                case sv: Seq[Any] => sv.length
                case sv: Vector => sv.size
                case _ =>
                  throw new Exception("您输入的数据类型不是array类型或vector类型")
              }
              val fields = Array.range(0, size).map(i => StructField(inputCol + "_" + i, DoubleType))

              (size, fields)
            } else {
              val names = colNames.get
              // @todo 需要确定数据中不存在该列名
              (names.length, names.map(s => StructField(s, DoubleType)))
            }

            val rdd = rawDataFrame.rdd.map(row => {

              /**
                * 算法：
                * 1）等长数组 =>
                * 按部就班地处理
                * 2）非不等长数组 =>
                * i)生成列数由输入列名数或第一条数据决定
                * ii)数据处理过程中如果数组长度超出该数目不要，如果少于数目以对应空值填充
                */
              elementType match {
                case StringType =>
                  row(inputID) match {
                    case arr: Seq[String] =>
                      val values = new Array[String](size)
                      var i = 0
                      while (i < scala.math.min(arr.length, size)) {
                        values(i) = arr(i)
                        i += 1
                      }
                      Row.merge(row, Row.fromSeq(values))
                    case _ =>
                      throw new SparkException("您输入的数据类型不是Array嵌套String或Double类型")
                  }
                case DoubleType =>
                  row(inputID) match {
                    case arr: Seq[Double] =>
                      val values = new Array[Double](size)
                      var i = 0
                      while (i < scala.math.min(arr.length, size)) {
                        values(i) = arr(i)
                        i += 1
                      }
                      Row.merge(row, Row.fromSeq(values))
                    case _ =>
                      throw new SparkException("您输入的数据类型Array嵌套String或Double类型")
                  }
                case _: VectorUDT =>
                  val values = new Array[Double](size)
                  row(inputID) match {
                    case sv: SparseVector =>
                      sv.indices.foreach { index =>
                        if (index < size)
                          values(index) = sv.values(index)
                      }
                    case dv: DenseVector =>
                      var i = 0
                      while (i < scala.math.min(dv.size, size)) {
                        values(i) = dv.values(i)
                        i += 1
                      }
                    case _ =>
                      throw new SparkException("您输入的数据类型不是vector类型")
                  }
                  Row.merge(row, Row.fromSeq(values))
              }
            })

            val newSchema = StructType(rawSchema ++ fields)
            rawDataFrame.sqlContext.createDataFrame(rdd, newSchema)

        }

    }


    newDataFrame.show()

    newDataFrame.registerTempTable("<#zzjzRddName#>")
    newDataFrame.sqlContext.cacheTable("<#zzjzRddName#>")
    outputrdd.put("<#zzjzRddName#>", newDataFrame)

  }
}
