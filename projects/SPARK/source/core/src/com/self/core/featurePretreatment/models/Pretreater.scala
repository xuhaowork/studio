package com.self.core.featurePretreatment.models

import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.NullableFunctions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, NullableFunctions}

import scala.reflect.ClassTag

/**
  * editor: xuhao
  * date: 2018-06-08 08:30:00
  */

/**
  * 特征预处理的抽象类
  * ----
  * 脚本设计思路：
  * 1）实现了实际中预处理的算法；
  * 2）这些算法的[输入数据]和[输出数据]很相似（都至少一个DataFrame）；
  * 3）这些算法的[运行函数]和[参数]差异很大。
  * 因此 ->
  * 1）将[数据]、[输出函数run()]写在抽象类中；
  * 2）将[参数]、[运行函数]作为接口；
  * 3）为了预备将来可能加入新的方法，这里将输出类型做成了一个泛型（虽然目前只有一种输出———DataFrame）。
  * ----
  * 具体来讲，该脚本提供了：
  * 1)实现方法：设定参数和获取参数信息 @see [[setParams]] @see [[getParams]]
  * 2)实现方法：最后统一的输出 @see [[run()]]
  * 2)接口：运行函数的接口 @see [[runAlgorithm()]]
  * 3)接口：判定每个参数是否合法，@see [[setParams]]中的参数[checkParam]，该参数是一个函数
  * 4)接口：判定整个参数组是否满足应有的约束，@see [[paramsValid]]
  *
  * @param data 输入的主要变换的data(还可能有其他DataFrame输出)
  */
abstract class Pretreater[M <: PretreatmentOutput](val data: DataFrame) {

  /**
    * 参数组列表
    * 有一些非常常用的参数，约定一下参数名：
    * inputCol -> 输入列名
    * outputCol -> 输出列名
    */
  val params: java.util.HashMap[String, Any] = new java.util.HashMap[String, Any]()


  /** 参数信息以[[ParamInfo]]的形式传入 */
  final def setParams(paramInfo: ParamInfo, param: Any): this.type = {
    this.params.put(paramInfo.name, param)
    this
  }

  /** 参数信息以[[ParamInfo]]的形式传入 */
  final def setParams(paramInfo: ParamInfo,
                      param: Any,
                      checkParam: Function3[String, Any, String, Boolean] = (_, _, _) => true): this.type = {
    require(checkParam(paramInfo.name, param, paramInfo.annotation), s"您输入的参数不合法：${paramInfo.annotation}")
    this.params.put(paramInfo.name, param)
    this
  }


  /** 传入的参数信息为[[ParamInfo]]的形式 */
  final def getParams[T <: Any : ClassTag](paramInfo: ParamInfo): T = {

    lazy val exception = new IllegalArgumentException(s"您没有找到参数${paramInfo.annotation}")
    val value = this.params.getOrDefault(paramInfo.name, exception)
    lazy val castTypeException = (msg: String) =>
      new ClassCastException(s"${paramInfo.annotation} 不能转为指定类型${}, 具体信息: $msg")
    try {
      value.asInstanceOf[T]
    } catch {
      case e: Exception => throw castTypeException(e.getMessage)
    }
  }


  final def getParamsOrElse[T <: Any : ClassTag](paramInfo: ParamInfo, other: T): T = {
    val value = this.params.getOrDefault(paramInfo.name, other).asInstanceOf[T]
    lazy val castTypeException = (msg: String) =>
      new ClassCastException(s"${paramInfo.annotation} 不能转为指定类型${}, 具体信息: $msg")
    try {
      value.asInstanceOf[T]
    } catch {
      case e: Exception => throw castTypeException(e.getMessage)
    }
  }


  protected def paramsValid(): Boolean


  /**
    * [输出函数run()]
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    * @see [[com.zzjz.deepinsight.core.featurePretreatment.models.PretreatmentOutput]]
    *      里面至少有一个输出的DataFrame，还可能有一个额外的输出信息。
    */
  final def run(): M = {
    require(data.schema.fieldNames.distinct.length == data.schema.size,
      "数据中有模糊列名会影响后续操作, 请检查是否有同名列") // 检查数据是否有模糊列名
    paramsValid() // 检查参数组信息是否合法
    runAlgorithm() // 执行运行函数
  }

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  protected def runAlgorithm(): M

}

/**
  * 正则表达式分词
  */
class TokenizerByRegex(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.RegexTokenizer

    val inputCol = getParams[String](TokenizerByRegexParamsName.inputCol)
    val outputCol = getParams[String](TokenizerByRegexParamsName.outputCol)
    val gaps = getParamsOrElse[Boolean](TokenizerByRegexParamsName.gaps, true)
    val pattern = getParamsOrElse[String](TokenizerByRegexParamsName.pattern, "\\s+")
    val minTokenLength = getParamsOrElse[Int](TokenizerByRegexParamsName.minTokenLength, 0)

    val tokenizer = new RegexTokenizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setGaps(gaps)
      .setPattern(pattern)
      .setMinTokenLength(minTokenLength)
    val wordsData = try {
      tokenizer.transform(this.data)
    } catch {
      case e: Exception => throw new Exception("正则表达式分词转换过程中出现异常，" +
        s"请检查是否是正则表达式或者一些参数发生了错误，${e.getMessage}")
    }
    new UnaryOutput(wordsData)
  }
}


class CountWordVector(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  /** 2-2 向量计数器 */
  override protected def paramsValid(): Boolean = true

  override protected def runAlgorithm(): UnaryOutput = {

    import org.apache.spark.ml.feature.CountVectorizer

    val inputCol = getParams[String](CountWordVectorParamsName.inputCol)
    val outputCol = getParams[String](CountWordVectorParamsName.outputCol)
    val vocabSize = getParamsOrElse[Int](CountWordVectorParamsName.vocabSize, 1 << 18) // 默认 2^18
    val minDf = getParamsOrElse[Double](CountWordVectorParamsName.minDf, 1.0) // 默认1.0
    val minTf = getParamsOrElse[Double](CountWordVectorParamsName.minTf, 1.0) // 默认1.0 这里会设置一次，因为可能持久化

    val model = new CountVectorizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setVocabSize(vocabSize)
      .setMinDF(minDf)
      .setMinTF(minTf)
      .fit(data)

    val newDataFrame = model
      .transform(data)
    new UnaryOutput(newDataFrame)
  }

}


class HashTF(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {

  import org.apache.spark.ml.feature.HashingTF


  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {


    val inputCol = getParams[String](HashTFParamsName.inputCol)
    val outputCol = getParams[String](HashTFParamsName.outputCol)
    val numFeatures = getParamsOrElse[Int](HashTFParamsName.numFeatures, 1 << 18) // 默认 1 << 18, 不能超过int的最大值

    val tfDataFrame = new HashingTF()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setNumFeatures(numFeatures)
      .transform(data)
    new UnaryOutput(tfDataFrame)
  }

}


class WordToVector(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {

  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.Word2Vec

    val inputCol = getParams[String](WordToVectorParamsName.inputCol) // 这个需要在模型中再设置一次
    val outputCol = getParams[String](WordToVectorParamsName.outputCol)

    val trainData = getParamsOrElse[DataFrame](WordToVectorParamsName.trainData, data)
    // 如果不输入默认训练数据就是预测数据

    val trainInputCol = getParamsOrElse[String](WordToVectorParamsName.trainInputCol,
      inputCol) // 如果不输入默认训练数据就是预测数据

    val trainOutputCol = getParamsOrElse[String](WordToVectorParamsName.trainOutputCol,
      outputCol) // 如果不输入默认训练数据就是预测数据

    val vocabSize = getParamsOrElse[Int](WordToVectorParamsName.vocabSize, 100) // 默认 2^18
    //      val windowSize = getParamsOrElse[Int](WordToVectorParamsName.windowSize, 5) // 默认1.0
    val stepSize = getParamsOrElse[Double](WordToVectorParamsName.stepSize, 0.025) // 默认1.0 这里会设置一次，因为可能持久化
    val numPartitions = getParamsOrElse[Int](WordToVectorParamsName.numPartitions, 1) // 默认1.0 这里会设置一次，因为可能持久化
    val numIterations = getParamsOrElse[Int](WordToVectorParamsName.numIterations, 1) // 默认1.0 这里会设置一次，因为可能持久化
    val minCount = getParamsOrElse[Int](WordToVectorParamsName.minCount, 5)
    val seed = getParams[Long](WordToVectorParamsName.seed)

    val model = new Word2Vec()
      .setInputCol(trainInputCol)
      .setOutputCol(trainOutputCol)
      .setVectorSize(vocabSize)
      .setMinCount(minCount)
      .setMaxIter(numIterations)
      //        .setWindowSize(windowSize)
      .setSeed(seed)
      .setNumPartitions(numPartitions)
      .setStepSize(stepSize)
      .fit(trainData)

    val newDataFrame = model
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .transform(data)
    new UnaryOutput(newDataFrame)
  }

  override protected def paramsValid(): Boolean = true
}


class StopWordsRmv(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.StopWordsRemover

    val inputCol = getParams[String](StopWordsRemoverParamsName.inputCol)
    val outputCol = getParams[String](StopWordsRemoverParamsName.outputCol)
    val caseSensitive = getParamsOrElse[Boolean](StopWordsRemoverParamsName.caseSensitive, false)
    val stopWords = getParams[Array[String]](StopWordsRemoverParamsName.stopWords) // 可以手动、或者选择英语或汉语
    // 英语：StopWords.English
    // 汉语：
    val newDataFrame = new StopWordsRemover()
      .setInputCol(inputCol)
      .setOutputCol(outputCol).setCaseSensitive(caseSensitive)
      .setStopWords(stopWords) // 停用词 StopWords.English
      .transform(data)

    new UnaryOutput(newDataFrame)

  }
}

class NGramMD(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.NGram

    val inputCol = getParams[String](NGramParamsName.inputCol)
    val outputCol = getParams[String](NGramParamsName.outputCol)
    val n = getParamsOrElse[Int](NGramParamsName.n, 3)

    val newDataFrame = new NGram()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setN(n)
      .transform(data)
    new UnaryOutput(newDataFrame)
  }
}

class Discretizer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */

  def checkFrameSize(boxesNum: Long): Boolean = {
    val akkaFrameSize = util.Try(
      data.sqlContext.sparkContext.getConf.getSizeAsMb("spark.akka.frameSize")).toOption
    if (akkaFrameSize.isDefined) {
      val size = (1 << 17).toLong * akkaFrameSize.get // 17 = 20 - 3, a double = 2^3 Bytes
      size > boxesNum
    } else {
      println("没有获得akka.frameSize的参数，未能判断等深或自定义的分箱边界是否超过结点间传输限制。")
      true
    }
  }

  override protected def runAlgorithm(): UnaryOutput = {

    val inputCol = getParams[String](DiscretizerParams.inputCol)
    val outputCol = getParams[String](DiscretizerParams.outputCol)
    val discretizeFormat = getParams[String](DiscretizerParams.discretizeFormat)
    // 分为等宽[byWidth]、自定义[selfDefined]

    discretizeFormat match {
      case "byWidth" =>
        val phase = getParams[Double](DiscretizerParams.phase)
        val width = getParams[Double](DiscretizerParams.width)
        require(width > 0, "等宽分箱宽度需要大于0")

        val binningByWidth = udf((d: Double) => scala.math.floor((d - phase) / width))
        new UnaryOutput(data.withColumn(outputCol, binningByWidth(col(inputCol))))

      case "selfDefined" =>
        import org.apache.spark.ml.feature.Bucketizer

        val bucketsAddInfinity = getParamsOrElse[Boolean](DiscretizerParams.bucketsAddInfinity, false)
        val buckets = if (bucketsAddInfinity) {
          Double.MinValue +: getParams[Array[Double]](DiscretizerParams.buckets).sorted :+ Double.MaxValue
        } else {
          getParams[Array[Double]](DiscretizerParams.buckets).sorted :+ Double.MaxValue
        }

        val newDataFrame = new Bucketizer()
          .setInputCol(inputCol)
          .setOutputCol(outputCol)
          .setSplits(buckets)
          .transform(data)

        new UnaryOutput(newDataFrame)
    }
  }
}


class OneHotCoder(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.OneHotEncoder

    val inputCol = getParams[String](OneHotCoderParams.inputCol)
    val outputCol = getParams[String](OneHotCoderParams.outputCol)
    val dropLast = getParamsOrElse[Boolean](OneHotCoderParams.dropLast, false)

    val newDataFrame = new OneHotEncoder()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setDropLast(dropLast)
      .transform(data)

    new UnaryOutput(newDataFrame)
  }
}

class IDFTransformer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true


  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    /** 2-3 tf-idf转换 */
    import org.apache.spark.ml.feature.IDF

    val inputCol = getParams[String](IDFTransformerParams.inputCol)
    val outputCol = getParams[String](IDFTransformerParams.outputCol)
    val minDocFreq = getParamsOrElse[Int](IDFTransformerParams.minDocFreq, 0)

    val idf = new IDF()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setMinDocFreq(minDocFreq) // 最低统计词频
    val model = idf.fit(data)

    val newDataFrame = model.setInputCol(inputCol)
      .setOutputCol(outputCol)
      .transform(data)
    new UnaryOutput(newDataFrame)
  }
}


/** 低变异性数值特征索引化 */
class VectorIndexerTransformer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.VectorIndexer

    val inputCol = getParams[String](VectorIndexerParams.inputCol)
    val outputCol = getParams[String](VectorIndexerParams.outputCol)
    val maxCategories = getParamsOrElse[Int](VectorIndexerParams.maxCategories, 20)
    val model = new VectorIndexer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setMaxCategories(maxCategories)
      .fit(data)

    val newDataFrame = model.setInputCol(inputCol)
      .setOutputCol(outputCol)
      .transform(data)

    new UnaryOutput(newDataFrame)
  }
}


class PCATransformer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.PCA

    val inputCol = getParams[String](PCAParams.inputCol)
    val outputCol = getParams[String](PCAParams.outputCol)

    val trainData = getParamsOrElse[DataFrame](PCAParams.trainData, data)
    val trainInputCol = getParamsOrElse[String](PCAParams.trainInputCol, inputCol)
    val trainOutputCol = getParamsOrElse[String](PCAParams.trainOutputCol, outputCol)
    val p = getParams[Int](PCAParams.p)

    val model = new PCA()
      .setInputCol(trainInputCol)
      .setOutputCol(trainOutputCol)
      .setK(p)
      .fit(trainData)

    val newDataFrame = model.setInputCol(inputCol).setOutputCol(outputCol).transform(data)
    new UnaryOutput(newDataFrame)
  }
}


class PlynExpansionTransformer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.PolynomialExpansion

    val inputCol = getParams[String](PlynExpansionParams.inputCol)
    val outputCol = getParams[String](PlynExpansionParams.outputCol)
    val degree = getParams[Int](PlynExpansionParams.degree)

    val newDataFrame = new PolynomialExpansion()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setDegree(degree)
      .transform(data)

    new UnaryOutput(newDataFrame)
  }
}


class DCTTransformer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.DCT

    val inputCol = getParams[String](DCTParams.inputCol)
    val outputCol = getParams[String](DCTParams.outputCol)
    val inverse = getParamsOrElse[Boolean](DCTParams.inverse, false)

    val newDataFrame = new DCT()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setInverse(inverse)
      .transform(data)

    new UnaryOutput(newDataFrame)
  }
}


class StringIndexTransformer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.StringIndexer

    val inputCol = getParams[String](StringIndexParams.inputCol)
    val outputCol = getParams[String](StringIndexParams.outputCol)

    val newData = try {
      data.withColumn(inputCol, col(inputCol).cast("string"))
    } catch {
      case e: Exception => throw new Exception(s"您输入的数据列${inputCol}不能转为double类型，${e.getMessage}")
    }

    val model: StringIndexerModel = new StringIndexer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .fit(newData)
    val newDataFrame = model
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .transform(newData)

    new UnaryOutput(newDataFrame)
  }
}


class IndexerStringTransformer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.IndexToString

    val inputCol = getParams[String](IndexToStringParams.inputCol)
    val outputCol = getParams[String](IndexToStringParams.outputCol)

    val labels = getParams[Array[String]](IndexToStringParams.labels)

    val model = new IndexToString()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setLabels(labels)

    val newData = try {
      data.filter(s"$inputCol < ${model.getLabels.length} and $inputCol >= 0")
    } catch {
      case e: Exception => throw new Exception(s"数据过滤中失败，" +
        s"请检查${inputCol}是否存在或者类型是否是double类型，${e.getMessage}")
    }

    val newDataFrame = model
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .transform(newData)

    new UnaryOutput(newDataFrame)
  }
}


class ElementProduct(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.ElementwiseProduct
    import org.apache.spark.ml.linalg.Vectors


    val inputCol = getParams[String](ElementProductParams.inputCol)
    val outputCol = getParams[String](ElementProductParams.outputCol)
    val weights = getParams[Array[Double]](ElementProductParams.weight)

    val transformingVector = Vectors.dense(weights)
    val newDataFrame = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .transform(data)

    new UnaryOutput(newDataFrame)
  }
}


class VectorAssembleTransformer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.VectorAssemblerExtend
    val inputCols = getParams[Array[String]](VectorAssembleParams.inputCol)
    val outputCol = getParams[String](VectorAssembleParams.outputCol)
    val types = Array(DoubleType, BooleanType, IntegerType, FloatType, LongType, ArrayType(StringType, true),
      ArrayType(StringType, false), ArrayType(DoubleType, true), ArrayType(DoubleType, false))

    require(inputCols.forall(data.schema.fieldNames contains _), "有列名不在数据表中")
    data.schema.foreach(field =>
      if (inputCols contains field.name) {
        if (!field.dataType.isInstanceOf[VectorUDT]) {
          require(types contains field.dataType, s"集成的数据类型必须是Vector类型或${types.map(_.toString).mkString(",")}")
        }
      }
    )

    val newDataFrame = new VectorAssemblerExtend()
      .setInputCols(inputCols)
      .setOutputCol(outputCol)
      .transform(data)

    new UnaryOutput(newDataFrame)
  }
}


class ChiFeatureSqSelector(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.mllib.feature.ChiSqSelector

    val inputCol = getParams[String](ChiFeatureSqSelectorParams.inputCol)
    val outputCol = getParams[String](ChiFeatureSqSelectorParams.outputCol)
    val labeledCol = getParams[String](ChiFeatureSqSelectorParams.labeledCol)
    val topFeatureNums = getParams[Int](ChiFeatureSqSelectorParams.topFeatureNums)

    val chiSqModel = new ChiSqSelector(topFeatureNums).fit(
      data.select(labeledCol, inputCol).na.drop("any").rdd.map(
        row => {
          val label = if(row.isNullAt(0)) 0.0 else row.get(0).toString.toDouble
          val features = row.getAs[Vector](1)
          LabeledPoint(label, features)
        }))

    val transformUDF = NullableFunctions.udf((v: Vector) => chiSqModel.transform(v))
    val newDataFrame = data.withColumn(outputCol, transformUDF(col(inputCol)))

    new UnaryOutput(newDataFrame)
  }
}


class VectorIndices(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.VectorSlicer

    val inputCol = getParams[String](VectorIndicesParams.inputCol)
    val outputCol = getParams[String](VectorIndicesParams.outputCol)
    val indices = getParams[Array[Int]](VectorIndicesParams.indices)

    val newDataFrame = new VectorSlicer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setIndices(indices)
      .transform(data)

    new UnaryOutput(newDataFrame)
  }
}


class NormalizerTransformer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    /** 正则化 */
    import org.apache.spark.ml.feature.Normalizer

    val inputCol = getParams[String](NormalizerParams.inputCol)
    val outputCol = getParams[String](NormalizerParams.outputCol)
    val p = getParams[Double](NormalizerParams.p)

    val newDataFrame = new Normalizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setP(p)
      .transform(data)

    new UnaryOutput(newDataFrame)
  }
}


class StandardScaleTransformer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.StandardScaler

    val inputCol = getParams[String](StandardScaleParam.inputCol)
    val outputCol = getParams[String](StandardScaleParam.outputCol)
    val withStd = getParams[Boolean](StandardScaleParam.withMean)
    val withMean = getParams[Boolean](StandardScaleParam.withStd)

    val newDataFrame = new StandardScaler()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setWithStd(withStd)
      .setWithMean(withMean)
      .fit(data)
      .transform(data)

    new UnaryOutput(newDataFrame)
  }
}


class MinMaxScaleTransformer(override val data: DataFrame) extends Pretreater[UnaryOutput](data) {
  override protected def paramsValid(): Boolean = true

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  override protected def runAlgorithm(): UnaryOutput = {
    import org.apache.spark.ml.feature.MinMaxScaler
    val inputCol = getParams[String](MinMaxScaleParam.inputCol)
    val outputCol = getParams[String](MinMaxScaleParam.outputCol)
    val max = getParams[Double](MinMaxScaleParam.max)
    val min = getParams[Double](MinMaxScaleParam.min)

    val newDataFrame = new MinMaxScaler()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setMax(max)
      .setMin(min)
      .fit(data)
      .transform(data)

    new UnaryOutput(newDataFrame)
  }
}


case class ParamInfo(name: String, annotation: String) // 参数判断写在类外面了

class ParamsName {
  /** 输入列名 */
  val inputCol = ParamInfo("inputCol", "预处理列名")

  /** 输出列名 */
  val outputCol = ParamInfo("outputCol", "输出列名")
}


class ParamsNameFromSavableModel extends ParamsName {
  val loadModel = ParamInfo("loadModel", "是否从持久化引擎中获取词频模型") // 是/否
  /** if [[loadModel]] == 是：需要进一步的训练信息如下 */
  val loadPath = ParamInfo("loadPath", "模型读取路径") // end if

  /** if [[loadModel]] == 否：需要进一步的训练信息如下 */
  val trainData = ParamInfo("trainData", "训练数据")
  val trainInputCol = ParamInfo("trainInputCol", "训练数据列名")
  val trainOutputCol = ParamInfo("trainOutputCol", "训练数据输出列名")
  /** 是否保存新训练的模型 */
  val saveModel = ParamInfo("saveModel", "是否将模型保存到持久化引擎中") // 是/否
  /** if[[saveModel]] == 是 */
  val savePath = ParamInfo("savePath", "模型保存路径") // end if // end if

}


object TokenizerByRegexParamsName extends ParamsName {
  /** 是以此为分隔还是以此为匹配类型 */
  val gaps = ParamInfo("gaps", "是以此为分隔还是以此为匹配类型")

  /** 匹配的模式 */
  val pattern = ParamInfo("pattern", "匹配的模式")

  /** 是否将英文转为小写 */
  val toLowerCase = ParamInfo("toLowerCase", "是否将英文转为小写")

  /** 最小分词长度 --不满足的将会被过滤掉 */
  val minTokenLength = ParamInfo("minTokenLength", "最小分词长度")
}

object CountWordVectorParamsName extends ParamsName {
  val loadModel = ParamInfo("loadModel", "是否从持久化引擎中获取词频模型") // 是/否
  /** if [[loadModel]] == 是：需要进一步的训练信息如下 */
  val loadPath = ParamInfo("loadPath", "模型读取路径") // end if

  /** if [[loadModel]] == 否：需要进一步的训练信息如下 */
  val trainData = ParamInfo("trainData", "训练数据")
  val trainInputCol = ParamInfo("trainInputCol", "训练数据列名")
  val trainOutputCol = ParamInfo("trainOutputCol", "训练数据输出列名")
  val vocabSize = ParamInfo("VocabSize", "词汇数")
  val minDf = ParamInfo("minDf", "最小文档频率")
  val minTf = ParamInfo("minTf", "最小词频")
  /** 是否保存新训练的模型 */
  val saveModel = ParamInfo("saveModel", "是否将模型保存到持久化引擎中") // 是/否
  /** if[[saveModel]] == 是 */
  val savePath = ParamInfo("savePath", "模型保存路径") // end if // end if

}


object HashTFParamsName extends ParamsName {
  val numFeatures = ParamInfo("numFeatures", "词汇数")
}


object WordToVectorParamsName extends ParamsName {
  val loadModel = ParamInfo("loadModel", "是否从持久化引擎中获取词频模型") // 是/否
  /** if [[loadModel]] == 是：需要进一步的训练信息如下 */
  val loadPath = ParamInfo("loadPath", "模型读取路径") // end if

  /** if [[loadModel]] == 否：需要进一步的训练信息如下 */
  val trainData = ParamInfo("trainData", "训练数据")
  val trainInputCol = ParamInfo("trainInputCol", "训练数据列名")
  val trainOutputCol = ParamInfo("trainOutputCol", "训练数据输出列名")
  val vocabSize = ParamInfo("VocabSize", "词汇数")
  val minCount = ParamInfo("minCount", "最小词频")
  val windowSize = ParamInfo("windowSize", "gram宽度")
  val stepSize = ParamInfo("stepSize", "步长")
  val numPartitions = ParamInfo("numPartitions", "并行度")
  val numIterations = ParamInfo("numIterations", "执行次数")
  val seed = ParamInfo("seed", "随机数种子")


  /** 是否保存新训练的模型 */
  val saveModel = ParamInfo("saveModel", "是否将模型保存到持久化引擎中") // 是/否
  /** if[[saveModel]] == 是 */
  val savePath = ParamInfo("savePath", "模型保存路径") // end if // end if

}

object StopWordsRemoverParamsName extends ParamsName {
  val caseSensitive = ParamInfo("caseSensitive", "是否区分大小写")
  val stopWords = ParamInfo("numIterations", "停用词") // 这里严格接受Array[String]类型的停用词，选择英语汉语停用词典放在外层
}

object NGramParamsName extends ParamsName {
  val n = ParamInfo("n", "gram数值")
}

object DiscretizerParams extends ParamsName {
  val discretizeFormat = ParamInfo("discretizeFormat", "离散化模式") // 分为等宽[byWidth]、等深[byDepth]、自定义[selfDefined]

  val phase = ParamInfo("phase", "周期初始相位——某个箱子的起始数值") // if discretizeFormat == byWidth
  val width = ParamInfo("width", "箱子宽度") // if discretizeFormat == byWidth

  val depth = ParamInfo("depth", "深度") // if discretizeFormat == byDepth  --和boxesNum之间二选一
  val boxesNum = ParamInfo("boxesNum", "箱子数") // if discretizeFormat == byDepth

  val buckets = ParamInfo("buckets", "分箱边界") // if discretizeFormat == byDepth
  val bucketsAddInfinity = ParamInfo("bucketsAddInfinity", "是否以极小值作为分箱边界的一部分") // if discretizeFormat == byDepth

}

object OneHotCoderParams extends ParamsName {
  val dropLast = ParamInfo("dropLast", "是否去掉最后一个index")

}

object IDFTransformerParams extends ParamsNameFromSavableModel {
  val minDocFreq = ParamInfo("minDocFreq", "最低文档频率") // 当某个数据出现的文档数（记录数）低于该值时会被过滤掉。
}

object VectorIndexerParams extends ParamsNameFromSavableModel {

  val maxCategories = ParamInfo("maxCategories", "离散型特征频次阈值")
}

object PCAParams extends ParamsNameFromSavableModel {

  val p = ParamInfo("p", "PCA的主成分数")
}

object PlynExpansionParams extends ParamsName {
  val degree = ParamInfo("degree", "展开式的幂")
}


object DCTParams extends ParamsName {
  val inverse = ParamInfo("inverse", "展开式的幂")
}


object StringIndexParams extends ParamsNameFromSavableModel {
  val handleInvalid = ParamInfo("handleInvalid", "怎样处理匹配不上的数据")
}


object IndexToStringParams extends ParamsNameFromSavableModel {
  val labels = ParamInfo("labels", "转换标签")
}


object ElementProductParams extends ParamsName {
  val weight = ParamInfo("weight", "权重")
}

object VectorAssembleParams extends ParamsName

object ChiFeatureSqSelectorParams extends ParamsNameFromSavableModel {
  val labeledCol = ParamInfo("labeledCol", "标签列")
  val topFeatureNums = ParamInfo("topFeatureNums", "选取的特征数")
}


object VectorIndicesParams extends ParamsName {
  val indices = ParamInfo("indices", "选择特征的位置索引")
}

object NormalizerParams extends ParamsName {
  val p = ParamInfo("p", "正则化阶数")
}


object StandardScaleParam extends ParamsName {
  val withStd = ParamInfo("withStd", "归一化标准差")
  val withMean = ParamInfo("withMean", "归一化均值")
}

object MinMaxScaleParam extends ParamsName {
  val min = ParamInfo("min", "最小值")
  val max = ParamInfo("max", "最大值")
}





