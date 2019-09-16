package com.self.core.confidenceInterval

import com.self.core.baseApp.myAPP

object confidenceIntervalMain extends myAPP {
  def createData() = {

  }


  override def run(): Unit = {
    import com.google.gson.Gson
    import com.self.core.featurePretreatment.utils.Tools
    import org.apache.spark.SparkException
    import org.apache.spark.mllib.random.RandomRDDs

    /** 1)解析参数 */
    val jsonparam = "<#zzjzParam#>"
    val gson = new Gson()
    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    val tableName = p.get("inputTableName")
    val valCol = p.get("valCol")
    val confidenceLevel = try {
      p.get("confidenceLevel").toDouble
    } catch {
      case e: Exception => throw new Exception(s"获得置信水平参数时异常，具体信息${e.getMessage}")
    }
    require(confidenceLevel >= 0.0 && confidenceLevel <= 1.0, s"置信水平只能是0到1之间的数值, 而您输入的是$confidenceLevel")

    val z1 = outputrdd
    val data = z1.get(tableName).asInstanceOf[org.apache.spark.sql.DataFrame].na.drop()
    data.registerTempTable("data")

    import org.apache.spark.sql.columnUtils.DataTypeImpl._
    Tools.columnExists(valCol, data, true)
    require(data.schema(valCol).dataType in Seq("numeric", "string"), "您输入的数据类型只能是string类型或者数值类型")

    /** 2)求均值/标准差 */
    val sqlContext = data.sqlContext
    val dataset = sqlContext.sql("select " + "`" + valCol + "`" + " from `data`").rdd.map(row =>
      try {
        row(0).toString.toDouble
      } catch {
        case e: Exception => throw new SparkException(s"数据中有不能转为数值的数据${row(0).toString}, 具体异常${e.getMessage}")
      })
    val mean = dataset.mean()
    val std = dataset.sampleStdev()

    /** 3)根据z统计量计算执行区间 */
    val N = 100000
    val layerNum = 100
    val z_statistic = (1 to layerNum).map { _ =>
      RandomRDDs.normalRDD(sqlContext.sparkContext, N).sortBy(x => - x).take(((1 - confidenceLevel) / 2 * N).toInt).min
    }.toArray.sum / layerNum

    //get confidence interval
    val confidenceInterval = (mean - std * z_statistic, mean + std * z_statistic)
    import sqlContext.implicits._
    val result = sqlContext.sparkContext.parallelize(Array(confidenceInterval)).toDF("lowerbound", "upperbound")

    /** 4)输出结果 */
    result.cache()
    outputrdd.put("<#zzjzRddName#>", result)
    result.registerTempTable("<#zzjzRddName#>")
    sqlContext.cacheTable("<#zzjzRddName#>")


    import org.apache.commons.math3.stat.interval.NormalApproximationInterval


  }
}
