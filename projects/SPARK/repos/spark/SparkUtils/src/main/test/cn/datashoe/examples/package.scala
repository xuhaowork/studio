package cn.datashoe

import java.util.Properties

import breeze.linalg.{*, CSCMatrix, DenseMatrix, DenseVector}
import cn.datashoe.BLAS.{MatrixOpsWithARPACK, MatrixOpsWithBreeze, MatrixOpsWithLAPACK}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import smile.clustering.KMeans
import smile.math.distance.EuclideanDistance
import smile.neighbor.{CoverTree, KDTree}

package object examples {
  /** 将string转为spark sql DataType类型 */
  def parseDataType(): Unit = {
    println(CatalystSqlParser.parseDataType("string"))
  }

  /** spark读取其他格式文件 */
  def readGBK(): Unit = {
    import org.apache.hadoop.io.{LongWritable, Text}
    import org.apache.hadoop.mapred.TextInputFormat

    val sc: SparkContext = null
    val path: String = null
    sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
  }

  /** 写入mysql --加入了batch读写: 提高性能 */
  def write2Mysql(): Unit = {
    val host = ""
    val port = ""
    val user = ""
    val password = ""
    val base = ""
    val tableNameNew = ""
    val saveMode = ""

    val df: DataFrame = null

    val url = s"jdbc:mysql://$host:$port/$base?rewriteBatchedStatements=true&useUnicode=true&characterEncoding=UTF8"
    val pro = new Properties()
    pro.setProperty("user", user)
    pro.setProperty("password", password)
    pro.setProperty("batchsize", "50000")

    df.write.mode(saveMode).jdbc(url, tableNameNew, pro)
  }

  /** 各种数据库对应的jar */
  def dbJars(): Unit = {
    Map(
      "driver" -> "com.mysql.jdbc.Driver",
      "driver" -> "oracle.jdbc.driver.OracleDriver",
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
      "driver" -> "com.gbase.jdbc.Driver",
      "driver" -> "org.postgresql.Driver"
    )
  }


  def readHBase(): Unit = {
    val sqlc: SQLContext = null
    val tableName = "table"
    val family = "students"
    val column = "code"
    val catalogTemplate =
      s"""{
         |"table":{"namespace":"default", "name":"$tableName", "tableCoder":"PrimitiveType"},
         |"rowkey":"key",
         |"columns":{
         |"RowKey":{"cf":"rowkey", "col":"key", "type":"string"},
         |"id":{"cf":"$family", "col":"$column", "type":"string"}
         |}
         |}""".stripMargin


    //    import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog}
    import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
    val df = sqlc.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalogTemplate))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    df.rdd.persist(StorageLevel.MEMORY_ONLY_SER)

  }


  def spectualClustering(): Unit = {
    //def run(param: String): Any = {
    //    val isLocal = true
    //    val jsonParam = param
    val jsonParam =
    """<#zzjzParam#>"""
    val rddTableName =
      """<#zzjzRddName#>"""

    val isLocal = false
    var inputDF: DataFrame = null
    var outputDF: DataFrame = null

    val sqlc: SQLContext = null


    val tableName = ""
    val columns = Array("", "")
    val clusteringCnt = 4
    val neighborCnt = 3


    inputDF.cache()

    // 收集数据
    val inputData = inputDF.select(columns.head, columns.tail: _*).rdd.map(row => {
      row.toSeq.map(v => v.toString.toDouble).toArray
    }).collect()

    val n = inputData.length
    val dimSize = columns.length
    require(clusteringCnt < n, "聚类个数应小于原始数据长度")

    // 根据维度大小创建KD树或CoverTree
    val knn = if (dimSize < 10) {
      new KDTree[Array[Double]](inputData, inputData)
    } else {
      new CoverTree[Array[Double]](inputData, new EuclideanDistance)
    }


    // 计算邻居矩阵
    val connectivity = CSCMatrix.zeros[Double](n, n)
    for (i <- 0 until n) {
      connectivity(i, i) = 1
      knn.knn(inputData(i), neighborCnt).foreach(neighbor => {
        connectivity(i, neighbor.index) = 1
      })
    }


    // 计算相似矩阵
    val affinityMatrix = (connectivity + connectivity.t) * 0.5
    val w = for (i <- 0 until n) yield {
      val rows = MatrixOpsWithBreeze.getRowsByColIndex(affinityMatrix, i)
      (for (row <- rows) yield {
        affinityMatrix.data(row)
      }).sum - affinityMatrix(i, i)
    }

    // 计算度矩阵
    val D = DenseVector.zeros[Double](n)
    val reciprocalD = CSCMatrix.zeros[Double](n, n)
    for (i <- 0 until n) {
      D(i) = if (w(i) == 0) 1 else math.sqrt(w(i))
      reciprocalD(i, i) = if (w(i) == 0) 1 else 1 / math.sqrt(w(i))
    }

    // 计算标准化后的拉普拉斯矩阵
    val L = reciprocalD * affinityMatrix * reciprocalD
    for (i <- 0 until n) {
      L(i, i) = -2
    }

    //Stopwatch.start()

    // 对矩阵做LU分解, 用于解方程
    val (lu, p) = MatrixOpsWithLAPACK.lu(L)

    //Stopwatch.restart("begin arpack")

    // 计算特征向量
    val data = MatrixOpsWithARPACK.eigen(
      v => MatrixOpsWithLAPACK.solve(lu, p, v),
      n,
      clusteringCnt,
      tol = 0.0,
      maxIterations = 1000,
      sigma = 1.0
    )

    //Stopwatch.end()

    // 标准化特征向量
    val mData = new DenseMatrix[Double](clusteringCnt, n, data.flatten).t
    for (i <- 0 until n) {
      for (j <- 0 until clusteringCnt) {
        mData(i, j) /= D(i)
      }
    }

    // 对标准化后的特征向量做聚类
    val points = mData(*, ::).map(row => {
      row.toArray
    }).data

    val retLable = new KMeans(points, clusteringCnt).getClusterLabel

    val orginData = inputDF.collect()

    val sc: SparkContext = null

    val rdd = sc.makeRDD(for (i <- 0 until n) yield {
      Row.merge(orginData(i), Row(retLable(i)))
    })

    outputDF = sqlc.createDataFrame(rdd, StructType(inputDF.schema :+ StructField("聚类结果", IntegerType)))
    outputDF.show()

  }


}
