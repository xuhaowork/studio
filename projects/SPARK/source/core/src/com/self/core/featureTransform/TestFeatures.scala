package com.self.core.featureTransform



import com.self.core.baseApp.myAPP
import com.self.core.featurePretreatment.models._
import com.self.core.featurePretreatment.utils.Tools
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.linalg.{Vector => mlVector}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql.columnUtils.VectorTransformation.{transform2mlVector, transform2mllibVector}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}


/**
  * editor: xuhao
  * date: 2018-06-08 08:30:00
  */
object TestFeatures extends myAPP {

  object data1 {
    val data: DataFrame = {
      sqlc.createDataFrame(Seq(
        (0, "Hi I heard about Spark"),
        (0, "I wish Java could use case classes, which i like mostly"),
        (1, "Does Logistic regression models has a implicit params, Halt?")
      )).toDF("label", "sentence")
    }

    val inputCol = "sentence"
    val outputCol = "sentenceOut"

    val data1 = {
      val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
      val wordsData = tokenizer.transform(data)
      wordsData // label, words -- Seq[String]
    }
  }

  object data2 {
    val data: DataFrame = {
      sqlc.createDataFrame(Seq(
        (0, "Hello，小李子"),
        (0, "今天吃了吗？"),
        (1, "谁今天要请我吃饭？")
      )).toDF("label", "sentence")
    }

    val inputCol = "sentence"
    val outputCol = "sentenceOut"
  }


  object data3 {
    val data: DataFrame = sqlc.createDataFrame(Seq(
      (0, Array("a", "b", "c", "d")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "letters")

    val data2: DataFrame = sqlc.createDataFrame(Seq(
      (0, Array("a", "x", "f", "good")),
      (1, Array("a", "f", "b", "2", "a"))
    )).toDF("index", "words")

    val inputCol = "sentence"
    val outputCol = "sentenceOut"
  }

  object data4 {
    val data: DataFrame = sqlc.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

  }

  object data5 {
    val wordDataFrame: DataFrame = sqlc.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("label", "words")

    val inputCol = "words"
  }

  object data6 {
    val data: DataFrame = sqlc.createDataFrame(
      Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    ).toDF("id", "hour")
  }

  object data7 {
    val data: DataFrame = {
      val rawDataFrame = data6.data

      new Discretizer(rawDataFrame)
        .setParams(DiscretizerParams.inputCol, "hour")
        .setParams(DiscretizerParams.outputCol, "outputCol")
        .setParams(DiscretizerParams.discretizeFormat, "byWidth")
        .setParams(DiscretizerParams.phase, 0.0)
        .setParams(DiscretizerParams.width, 3.0)
        .run()
        .data
    }
  }


  object data8 {

    import org.apache.spark.mllib.linalg.Vectors

    val data: DataFrame = {
      sqlc.createDataFrame(
        Array(
          Vectors.dense(2.0, 1.0),
          Vectors.dense(0.0, 3.0),
          Vectors.dense(0.0, -3.0),
          Vectors.dense(2.0, 2.0)).map(Tuple1.apply)
      ).toDF("features")
    }

  }

  object data9 {

    import org.apache.spark.mllib.linalg.Vectors

    val data: DataFrame = sqlc.createDataFrame(
      Array(
        Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
        Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
        Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
      ).map(Tuple1.apply)
    ).toDF("pcaFeature")


    val data2: DataFrame = sqlc.createDataFrame(
      Array(
        Vectors.sparse(7, Seq((1, 1.0), (3, 7.0))),
        Vectors.dense(2.0, 0.0, 3.0, 4.0),
        Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
      ).map(Tuple1.apply)
    ).toDF("pcaFeature")

  }

  object data10 {
    val data: DataFrame = sqlc.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val inputCol: String = "category"
  }


  /** 一、属性类型特征提取 */
  /** 1.正则表达式分词 */
  def test1() = {
    val testData = data1
    val rawDataFrame = testData.data

    val inputCol = testData.inputCol
    val outputCol = testData.outputCol
    val gaps = false
    val minTokenLength = 0
    val pattern = """[Ii].*[Hh]"""

    val newDataFrame = new TokenizerByRegex(rawDataFrame)
      .setParams(TokenizerByRegexParamsName.inputCol, inputCol)
      .setParams(TokenizerByRegexParamsName.outputCol, outputCol)
      .setParams(TokenizerByRegexParamsName.gaps, gaps)
      .setParams(TokenizerByRegexParamsName.minTokenLength, minTokenLength)
      .setParams(TokenizerByRegexParamsName.toLowerCase, false)
      .setParams(TokenizerByRegexParamsName.pattern, pattern)
      .run()
      .data

    println(newDataFrame.schema)
    newDataFrame.show()
  }


  /** 2.词频统计 */
  def test2() = {
    data3.data.show()

    // 先全写上，后面用到啥写啥就可以运行了
    val newDataFrame = new CountWordVector(data3.data)
      .setParams(CountWordVectorParamsName.inputCol, "letters")
      .setParams(CountWordVectorParamsName.outputCol, "outputCol")
      //      .setParams(CountWordVectorParamsName.trainData, data3.data2)
      //      .setParams(CountWordVectorParamsName.trainInputCol, "words")
      //      .setParams(CountWordVectorParamsName.trainOutputCol, mediumCols)
      //      .setParams(CountWordVectorParamsName.loadModel, true)
      //      .setParams(CountWordVectorParamsName.saveModel, true)
      .setParams(CountWordVectorParamsName.vocabSize, 1 << 18)
      .setParams(CountWordVectorParamsName.minTf, 1.0)
      .setParams(CountWordVectorParamsName.minDf, 1.0)
      //      .setParams(CountWordVectorParamsName.savePath, "/data/wordCount/...")
      .run()
      .data

    newDataFrame.show()
  }


  /** 3.hash词频统计 */
  def test3() = {


    val sentenceData = data1.data1
    val numFeature = 10000 // 在由string转过来的时候需要判断一下是否越界
    val inputCol = "words"
    val outputCol = "wordsOutput"


    val newDataFrame = new HashTF(sentenceData)
      .setParams(HashTFParamsName.numFeatures, numFeature)
      .setParams(HashTFParamsName.inputCol, inputCol)
      .setParams(HashTFParamsName.outputCol, outputCol)
      .run()

    newDataFrame.data.show()
    newDataFrame.data
  }


  /** 4.Word2Vector */
  def test4() = {
    val rawDataFrame = data1.data1
    rawDataFrame.show()


    val newDataFrame = new WordToVector(rawDataFrame)
      .setParams(WordToVectorParamsName.inputCol, "words")
      .setParams(WordToVectorParamsName.outputCol, "wordsOutput")
      .setParams(WordToVectorParamsName.windowSize, 5)
      .setParams(WordToVectorParamsName.vocabSize, 1000)
      .setParams(WordToVectorParamsName.stepSize, 0.1)
      .setParams(WordToVectorParamsName.minCount, 1)
      .setParams(WordToVectorParamsName.numPartitions, 1)
      .setParams(WordToVectorParamsName.numIterations, 10)
      .setParams(WordToVectorParamsName.seed, 123L)
      .run()
      .data.withColumn("wordsOutput", transform2mllibVector(col("wordsOutput")))

    newDataFrame.show()

    println(newDataFrame.select("wordsOutput").collect().head.getAs[org.apache.spark.mllib.linalg.Vector](0).size)

  }


  /** 5. */
  def test5() = {

    val stopWordsFormat = "English" // "chinese", "byHand", "byFile"

    val stopWords = stopWordsFormat match {
      case "English" => StopWordsUtils.English
      case "Chinese" => StopWordsUtils.chinese
      case "byHand" => Array("", "")
      case "byFile" =>
        val path = "/data/dfe"
        val separator = ","
        sc.textFile(path).collect().flatMap(_.split(separator).map(_.trim))
    }

    val rawDataFrame = data4.data
    val newDataFrame = new StopWordsRmv(rawDataFrame)
      .setParams(StopWordsRemoverParamsName.inputCol, "raw")
      .setParams(StopWordsRemoverParamsName.outputCol, "rawOutput")
      .setParams(StopWordsRemoverParamsName.caseSensitive, false)
      .setParams(StopWordsRemoverParamsName.stopWords, stopWords)
      .run().data
    newDataFrame.show()
    println(StopWordsUtils.English.length)


  }

  def test6() = {
    /** n-gram */

    val rawDataFrame = data5.wordDataFrame
    val inputCol = data5.inputCol

    val newDataFrame = new NGramMD(rawDataFrame)
      .setParams(NGramParamsName.inputCol, inputCol)
      .setParams(NGramParamsName.outputCol, "wordsOutput")
      .setParams(NGramParamsName.n, 3)
      .run()
      .data
    newDataFrame.show()

  }


  /** 二、数值类型特征提取 */
  def test7() = {
    val rawDataFrame = data6.data

    val byWidthDF = new Discretizer(rawDataFrame)
      .setParams(DiscretizerParams.inputCol, "hour")
      .setParams(DiscretizerParams.outputCol, "outputCol")
      .setParams(DiscretizerParams.discretizeFormat, "byWidth")
      .setParams(DiscretizerParams.phase, 0.0)
      .setParams(DiscretizerParams.width, 4.0)
      .run()
      .data

    byWidthDF.show()

    val byDepthDF3 = new Discretizer(rawDataFrame)
      .setParams(DiscretizerParams.inputCol, "hour")
      .setParams(DiscretizerParams.outputCol, "outputCol")
      .setParams(DiscretizerParams.discretizeFormat, "selfDefined")
      .setParams(DiscretizerParams.buckets, Array(3.0, 9.0, 5.0))
      .setParams(DiscretizerParams.bucketsAddInfinity, true)
      .run()
      .data

    byDepthDF3.show()

  }


  def test8() = {
    val rawDataFrame = data7.data
    rawDataFrame.show()

    val newDataFrame = new OneHotCoder(rawDataFrame)
      .setParams(OneHotCoderParams.inputCol, "outputCol")
      .setParams(OneHotCoderParams.outputCol, "oneHot")
      .setParams(OneHotCoderParams.dropLast, true)
      .run()
      .data.withColumn("oneHot", transform2mllibVector(col("oneHot")))

    newDataFrame.show()

    newDataFrame.select("oneHot").rdd
      .map(_.get(0).asInstanceOf[org.apache.spark.mllib.linalg.Vector])
      .collect()
      .foreach(println)

  }


  // 这个需要ml.vector
  def test9(): DataFrame = {
    val rawDataFrame = test3().withColumn("wordsOutput", transform2mlVector(col("wordsOutput")))

    val inputCol = "wordsOutput"

    rawDataFrame.select(inputCol).rdd
      .map(_.get(0).asInstanceOf[org.apache.spark.ml.linalg.Vector])
      .collect()
      .foreach(println)

    val outputCol = "idfCol"
    val newDataFrame = new IDFTransformer(rawDataFrame)
      .setParams(IDFTransformerParams.inputCol, inputCol)
      .setParams(IDFTransformerParams.outputCol, outputCol)
      .setParams(IDFTransformerParams.loadModel, false)
      .setParams(IDFTransformerParams.minDocFreq, 1) // @todo: 必须为Int
      .setParams(IDFTransformerParams.saveModel, false)
      .run()
      .data

    newDataFrame.select(outputCol).rdd
      .map(_.get(0).asInstanceOf[org.apache.spark.ml.linalg.Vector])
      .collect()
      .foreach(println)
    newDataFrame
  }

  // 这个需要ml.vector
  def test10() = {
    val rawDataFrame = data8.data.withColumn("features", transform2mlVector(col("features")))

    rawDataFrame.show()

    val newDataFrame = new VectorIndexerTransformer(rawDataFrame)
      .setParams(VectorIndexerParams.inputCol, "features")
      .setParams(VectorIndexerParams.outputCol, "outPut")
      .setParams(VectorIndexerParams.loadModel, false)
      .setParams(VectorIndexerParams.maxCategories, 2) // @todo
      .setParams(VectorIndexerParams.loadModel, false)
      .run()
      .data

    newDataFrame.show()

  }

  // 这个需要ml.vector
  def test11(): Unit = {
    val rawDataFrame = data9.data.withColumn("pcaFeature", transform2mlVector(col("pcaFeature")))
    val inputCol = "pcaFeature"
    rawDataFrame.show()
    val newDataFrame = new PCATransformer(rawDataFrame)
      .setParams(PCAParams.inputCol, inputCol)
      .setParams(PCAParams.outputCol, "output")
      .setParams(PCAParams.loadModel, false)
      .setParams(PCAParams.saveModel, false)
      .setParams(PCAParams.p, 2) // 需要小于向量长度
      .run()
      .data
    newDataFrame.show()
  }

  def test12(): Unit = {
    val rawDataFrame = data9.data2.withColumn("pcaFeature", transform2mlVector(col("pcaFeature")))
    val inputCol = "pcaFeature"
    rawDataFrame.show()

    val newDataFrame = new PlynExpansionTransformer(rawDataFrame)
      .setParams(PlynExpansionParams.inputCol, inputCol)
      .setParams(PlynExpansionParams.outputCol, "output")
      .setParams(PlynExpansionParams.degree, 3)
      .run()
      .data
    newDataFrame.show()
  }

  def test13() = {
    val rawDataFrame = data9.data2.withColumn("pcaFeature", transform2mlVector(col("pcaFeature")))
    val inputCol = "pcaFeature"
    val outputCol = "outputCol"
    val inverse = false

    val newDataFrame = new DCTTransformer(rawDataFrame)
      .setParams(DCTParams.inputCol, inputCol)
      .setParams(DCTParams.outputCol, outputCol)
      .setParams(DCTParams.inverse, inverse)
      .run()
      .data

    newDataFrame.show()

  }

  def test14() = {
    val rawDataFrame = data10.data
    val inputCol = data10.inputCol

    rawDataFrame.show()

    val newDataFrame = new StringIndexTransformer(rawDataFrame)
      .setParams(StringIndexParams.inputCol, inputCol)
      .setParams(StringIndexParams.outputCol, "outputCol")
      .run()
      .data

    newDataFrame.show()


    import org.apache.spark.ml.feature.StringIndexer

    val df1 = sqlc.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"))
    ).toDF("id", "category")

    val df2 = sqlc.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"))
    ).toDF("id", "category")


    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df1).transform(df2)
    indexed.show()

  }


  def test15() = {
    val rawDataFrame = data10.data
    val inputCol = "id"

    val newDataFrame = new IndexerStringTransformer(rawDataFrame)
      .setParams(IndexToStringParams.inputCol, inputCol)
      .setParams(IndexToStringParams.outputCol, "outputCol")
      .setParams(IndexToStringParams.saveModel, false)
      .setParams(IndexToStringParams.labels, Array("a", "b", "c", "d", "e"))
      .run()
      .data

    newDataFrame.show()
  }


  def test16() = {
    import org.apache.spark.mllib.linalg.{Vector, Vectors}

    val rawDataFrame = sqlc.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0))))
      .toDF("id", "vector")
      .withColumn("vector", transform2mlVector(col("vector")))

    rawDataFrame.show()


    val inputCol = "vector"
    val outputCol = "outputCol"

    val vectorSize = rawDataFrame.select(inputCol).head.getAs[mlVector](0).size

    val transformingArray = Array(0.0, 1.0, 2.0)
    require(vectorSize == transformingArray.length, "您输入的权重需要和向量长度保持一致")

    val newDataFrame = new ElementProduct(rawDataFrame)
      .setParams(ElementProductParams.inputCol, inputCol)
      .setParams(ElementProductParams.outputCol, outputCol)
      .setParams(ElementProductParams.weight, transformingArray)
      .run()
      .data

    newDataFrame.show()
  }


  def test17() = {
    import org.apache.spark.mllib.linalg.Vectors
    val rawDataFrame = sqlc.createDataFrame(
      Seq((0, Array(1.0, 2.0, 3.0), Vectors.dense(10.0, 0.5), Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")
      .withColumn("mobile", transform2mlVector(col("mobile")))
      .withColumn("userFeatures", transform2mlVector(col("userFeatures")))

    val inputCols = Array("id", "hour", "mobile", "userFeatures")
    val outputCols = "outputCols"

    val newDataFrame = new VectorAssembleTransformer(rawDataFrame)
      .setParams(VectorAssembleParams.inputCol, inputCols)
      .setParams(VectorAssembleParams.outputCol, outputCols)
      .run()
      .data

    newDataFrame.show()

  }


  def test18() = {
    import org.apache.spark.mllib.linalg.Vectors

    val rawDataFrame = sqlc.createDataFrame(Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )).toDF("id", "features", "clicked").withColumn("features", transform2mlVector(col("features")))

    val newDataFrame = new ChiFeatureSqSelector(rawDataFrame).setParams(ChiFeatureSqSelectorParams.topFeatureNums, 2)
      .setParams(ChiFeatureSqSelectorParams.inputCol, "features")
      .setParams(ChiFeatureSqSelectorParams.labeledCol, "clicked")
      .setParams(ChiFeatureSqSelectorParams.outputCol, "outputCol")
      .run()
      .data

    newDataFrame.show()
  }


  def test19() = {
    val rawDataFrame = sqlc.createDataFrame(Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )).toDF("id", "features", "clicked").withColumn("features", transform2mlVector(col("features")))

    val newDataFrame = new VectorIndices(rawDataFrame)
      .setParams(VectorIndicesParams.inputCol, "features")
      .setParams(VectorIndicesParams.outputCol, "outputCol")
      .setParams(VectorIndicesParams.indices, Array(0, 3))
      .run()
      .data

    newDataFrame.show()
  }


  def test20() = {
    val rawDataFrame = sqlc.createDataFrame(Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )).toDF("id", "features", "clicked").withColumn("features", transform2mlVector(col("features")))

    val newDataFrame = new NormalizerTransformer(rawDataFrame)
      .setParams(NormalizerParams.inputCol, "features")
      .setParams(NormalizerParams.outputCol, "outputCol")
      .setParams(NormalizerParams.p, 2.0)
      .run()
      .data

    newDataFrame.show()
  }


  def test21() = {
    import org.apache.spark.ml.feature.StandardScaler

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(13.0, 14.0, 14.0, 14.0))

    val dataFrame = sqlc.createDataFrame(data.map(Tuple1.apply)).toDF("features")
      .withColumn("features", transform2mlVector(col("features")))

    dataFrame.show()

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(false)
      .setWithMean(true)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()

    /** min-max */
    import org.apache.spark.ml.feature.MinMaxScaler
    val scaler2 = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setMax(100.0)

    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel2 = scaler2.fit(dataFrame)

    // rescale each feature to range [min, max].
    val scaledData2 = scalerModel2.transform(dataFrame)
    scaledData2.show()


  }


  def test22() = {
    val data = Seq(
      (-1.0, Vectors.dense(Array(1.0, 3.0, 5.0)), 11F, Vectors.dense(Array(1.0, 2.0, -0.0))),
      (0.0, Vectors.dense(Array(1.0)), 13F, Vectors.dense(Array(1.0, 2.0, -0.0))),
      (2.0, Vectors.sparse(6, Array(1, 3), Array(0.0, 1.0)), 15F, Vectors.dense(Array(1.0, 2.0, -0.0)))
    )

    val dataFrame = sqlc.createDataFrame(data)
      .toDF("features", "notEqualLengthVector", "floatType", "equalDense")
      .withColumn("notEqualLengthVector", transform2mlVector(col("notEqualLengthVector")))
      .withColumn("equalDense", transform2mlVector(col("equalDense")))

    val df = new ChiFeatureSqSelector(dataFrame)
      .setParams(ChiFeatureSqSelectorParams.inputCol, "equalDense")
      .setParams(ChiFeatureSqSelectorParams.outputCol, "outputCol")
      .setParams(ChiFeatureSqSelectorParams.labeledCol, "floatType")
      .setParams(ChiFeatureSqSelectorParams.topFeatureNums, 2)
      .run()
      .data

    df.show()
  }


  def test23() = {
    /** 向量拆分 */
    val data = Seq(
      (-1.0, Vectors.dense(Array(1.0, 3.0, 5.0)), 11F, Vectors.dense(Array(1.0, 2.0, -0.0))),
      (0.0, Vectors.dense(Array(1.0)), 13F, Vectors.dense(Array(1.0, 2.0, -0.0))),
      (2.0, Vectors.sparse(6, Array(1, 3), Array(0.0, 1.0)), 15F, Vectors.dense(Array(1.0, 2.0, -0.0)))
    )

    val dataFrame = sqlc.createDataFrame(data).toDF("features", "notEqualLengthVector", "floatType", "equalDense")
      .withColumn("notEqualLengthVector", transform2mlVector(col("notEqualLengthVector")))
      .withColumn("equalDense", transform2mlVector(col("equalDense")))

    val inputCol = "equalDense"
    val colNames: Option[Array[String]] = None

    val rawSchema = dataFrame.schema
    val inputID = rawSchema.fieldIndex(inputCol)

    val (size, fields) = if (colNames.isEmpty) {
      val size = dataFrame.select(inputCol).head.get(0) match {
        case sv: Vector => sv.size
        case mlSv: mlVector => mlSv.size
        case _ =>
          throw new Exception("您输入的数据类型不是vector类型")
      }

      val fields = Array.range(0, size).map(i => StructField(inputCol + "_" + i, DoubleType))

      (size, fields)
    } else {
      val names = colNames.get
      // @todo 需要确定数据中不存在该列名
      (names.length, names.map(s => StructField(s, DoubleType)))
    }

    val rdd = dataFrame.rdd.map(row => {
      val rowArr = row.toSeq
      val values = new Array[Double](size)
      rowArr(inputID) match {
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
        case mlSv: mlVector =>
          Vectors.fromML(mlSv) match {
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
          }
        case _ =>
          throw new SparkException("您输入的数据类型不是vector类型")
      }

      Row.fromSeq(rowArr.slice(0, inputID) ++ values ++ rowArr.slice(inputID + 1, rowArr.length))
    })

    val newSchema = StructType(rawSchema.slice(0, inputID) ++ fields ++ rawSchema.slice(inputID + 1, rawSchema.length))
    val newDataFrame = dataFrame.sqlContext.createDataFrame(rdd, newSchema)
    newDataFrame.show()
  }


  def test24() = {
    /** 数组集成 */
    val data = Seq(
      (-1.0, Vectors.dense(Array(1.0, 3.0, 5.0)), 11F, Vectors.dense(Array(1.0, 2.0, -0.0))),
      (0.0, Vectors.dense(Array(1.0)), 13F, Vectors.dense(Array(1.0, 2.0, -0.0))),
      (2.0, Vectors.sparse(6, Array(1, 3), Array(0.0, 1.0)), 15F, Vectors.dense(Array(1.0, 2.0, -0.0)))
    )

    val dataFrame = sqlc.createDataFrame(data).toDF("features", "notEqualLengthVector", "floatType", "equalDense")
      .withColumn("notEqualLengthVector", transform2mlVector(col("notEqualLengthVector")))
      .withColumn("equalDense", transform2mlVector(col("equalDense")))

    val inputCols = Array("features", "notEqualLengthVector", "equalDense")
    val outputCol = "outputCol"
    val typeName = "string"

    val assembleFunc = Tools.getTheUdfByType(typeName)

    val newDataFrame = dataFrame.select(col("*"), assembleFunc(struct(inputCols.map(col): _*)).as(outputCol))
    newDataFrame.show()
    newDataFrame.schema.foreach(println)

  }


  /** 数组拆分 */
  def test25() = {
    val data = Seq(
      (-1.0, Array(1.0, 3.0, 5.0), 11F, Vectors.dense(Array(1.0, 2.0, -0.0))),
      (0.0, Array(1.0), 13F, Vectors.dense(Array(1.0, 2.0, -0.0))),
      (2.0, Array.empty[Double], 15F, Vectors.dense(Array(1.0, 2.0, -0.0)))
    )

    val dataFrame = sqlc.createDataFrame(data).toDF("features", "notEqualLengthArray", "floatType", "equalDense")
      .withColumn("equalDense", transform2mlVector(col("equalDense")))

    val inputCol = "notEqualLengthArray"
    val colNames: Option[Array[String]] = None

    val rawSchema = dataFrame.schema
    val inputID = rawSchema.fieldIndex(inputCol)

    /** 元素类型 */
    val elementType = rawSchema(inputCol).dataType match {
      case dt: ArrayType => dt.elementType
      case others => throw new Exception(s"您输入列${inputCol}的类型为${others.simpleString}，不是array类型")
    }
    require(elementType == StringType || elementType == DoubleType, s"数组需要为一层嵌套数组，并且其中元素类型" +
      s"只能是string或double，而您的类型为${elementType.simpleString}")

    /** schema处理 */
    val (size, fields) = if (colNames.isEmpty) {
      val size = dataFrame.select(inputCol).head.get(0) match {
        case sv: Seq[Any] => sv.length
        case others =>
          throw new Exception(s"您输入的数据类型不是数组类型，为${others.getClass.getName}")
      }
      require(size > 0, "您的数据第一条记录对应的数组为空，无法获得您想要生成列数")
      val fields = Array.range(0, size).map(i => StructField(inputCol + "_" + i, elementType))

      (size, fields)
    } else {
      val names = colNames.get
      // @todo 需要确定数据中不存在该列名
      (names.length, names.map(s => StructField(s, DoubleType)))
    }


    val rdd = dataFrame.rdd.map(row => {
      val rowArr = row.toSeq

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
          rowArr(inputID) match {
            case arr: Seq[String] =>
              val values = new Array[String](size)
              var i = 0
              while (i < scala.math.min(arr.length, size)) {
                values(i) = arr(i)
                i += 1
              }
              Row.fromSeq(rowArr.slice(0, inputID) ++ values ++ rowArr.slice(inputID + 1, rowArr.length))
            case _ =>
              throw new SparkException("您输入的数据类型不是Array嵌套String或Double类型")
          }
        case DoubleType =>
          rowArr(inputID) match {
            case arr: Seq[Double] =>
              val values = new Array[Double](size)
              var i = 0
              while (i < scala.math.min(arr.length, size)) {
                values(i) = arr(i)
                i += 1
              }
              Row.fromSeq(rowArr.slice(0, inputID) ++ values ++ rowArr.slice(inputID + 1, rowArr.length))
            case _ =>
              throw new SparkException("您输入的数据类型Array嵌套String或Double类型")
          }
      }

    })

    val newSchema = StructType(rawSchema.slice(0, inputID) ++ fields ++ rawSchema.slice(inputID + 1, rawSchema.length))
    val newDataFrame = dataFrame.sqlContext.createDataFrame(rdd, newSchema)
    newDataFrame.show()
  }


  override def run(): Unit = {
    //    test1()
    //
    //    test2()
    //
    //    test3()
    //
    //    test4()
    //
    //    test5()
    //
    //    test6()
    //
    //    test7()
    //
    //    test8()
    //
    //    test9()
    //
    //    test10()

    //    test11()
    //
    //    test12()
    //
    //    test13()
    //
    //    test14()
    //
    //    test15()

    //    test16()
    //
    //    test17()
    //
    //    test18()
    //
    //    test19()
    //
    //    test20()

    test21()

    test22()

    test23()

    test24()

    test25()

    //    import org.apache.spark.mllib.linalg.Vectors
    //    import org.apache.spark.sql.DataFrame
    //    val data = Seq(
    //      (-1.0, Vectors.dense(Array(1.0, 3.0, 5.0))),
    //      (0.0, Vectors.dense(Array(1.0))),
    //      (2.0, Vectors.sparse(1000000, Array(1, 3), Array(0.0, 1.0)).toDense)
    //    )
    //
    //    val dataFrame = sqlc.createDataFrame(data).toDF("features", "notEqualLengthVector")
    //
    //    dataFrame.show()
    //
    //    import org.apache.spark.mllib.linalg.VectorUDT
    //    println(dataFrame.select("equalDense").schema.head.dataType.typeName)

    //    println(Int.MaxValue + 1)
    //
    //
    //    val data = Seq(
    //      (-1.0, Vectors.dense(Array(1.0, 3.0, 5.0)), 11F, Vectors.dense(Array(1.0, 2.0, -0.0))),
    //      (0.0, Vectors.dense(Array(1.0)), 13F, Vectors.dense(Array(1.0, 2.0, -0.0))),
    //      (2.0, Vectors.sparse(6, Array(1, 3), Array(0.0, 1.0)), 15F, Vectors.dense(Array(1.0, 2.0, -0.0)))
    //    )
    //
    //    val dataFrame = sqlc.createDataFrame(data).toDF("features", "notEqualLengthVector", "floatType", "equalDense")
    //
    //    dataFrame.show()
    //    import org.apache.spark.mllib.linalg.Vector
    //    val sparseToDense = NullableFunctions.udf((v: Vector) => v match {
    //      case dv: DenseVector => dv
    //      case sv: SparseVector => sv.toDense
    //    })
    //
    //    val denseToSparse = NullableFunctions.udf((v: Vector) => v match {
    //      case dv: DenseVector => dv.toSparse
    //      case sv: SparseVector => sv
    //    })


    //    val vocabSizeString = "16^2"
    //
    //
    //
    //    val vocabSize = if (vocabSizeString contains '^') {
    //      try {
    //        vocabSizeString.split('^').map(_.trim.toDouble).reduceLeft((d, s) => scala.math.pow(d, s)).toInt
    //      } catch {
    //        case e: Exception => throw new Exception(s"您输入词汇数参数表达式中包含指数运算符^，" +
    //          s"但^两侧可能包含不能转为数值类型的字符，或您输入的指数过大超过了2^31，具体错误为.${e.getMessage}")
    //      }
    //    } else {
    //      try {
    //        vocabSizeString.toDouble.toInt
    //      } catch {
    //        case e: Exception => throw new Exception(s"输入的词汇数参数不能转为数值类型，具体错误为.${e.getMessage}")
    //      }
    //    }
    //
    //    println(vocabSize)


  }

}
