package cn.datashoe.dataWrangling

import cn.datashoe.sparkUtils.DataSimulate
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, Row}
import org.scalatest.FunSuite

// @todo: 加入对特殊字符列名的兼容
class PivotSuite extends FunSuite {
  test("字符串格式化") {
    val precision = 0
    val format = "%." + precision + "f%%"
    println("字符串格式化:" + format.format(0.77788889999 * 100))

    //    val data = DataSimulate.salaryData(6000, 4)
    //    data.show()
    //
    //    import org.apache.spark.sql.functions._
    //    val result = data.groupBy("年龄").pivot("职业").agg(mean(col("薪资")))
    //    result.show()
    //
    //    result.schema.apply("").dataType
    //
    //    import org.apache.spark.sql.types.{DataType, DoubleType, Decimal}
    //    val da: DataType = null
    //
    //
    //
    //    println(DoubleType.typeName)
    //    println(DoubleType.simpleString)
    //    val column = col("薪资")


  }

  test("功能组织") {
    // 采用按键式的功能组织
    // ----
    // 行汇总 rowAgg
    // 列汇总 colAgg
    // 缺失值补全为0 naFill
    // 排序顺序 ascending
    // 列顺序 orderMode : byValueSum byLabel
    // 限定列数 colsLimitNum
    // 限定列标签值 colsLabels


    // 列类型和值的要求:
    // 1)行标签对应列 | 可以多列    | 可以任意
    // 2)列标签对应列 | 单列        | 可以任意       | 缺失值会drop掉
    // 3)值对应列     | 单列        | 需要是数值类型  | 缺失值可以转为0


    // 输入
    // ----
    // 数据
    // 行标签对应列名 一个或多个
    // 列标签对应列名 一个
    // 值对应列名 一个

    val rowsLabelColName = Array("年龄", "职业")
    val colsLabelColName = "婚姻"
    val valueColName = "薪资"


    val orderMode = "byValueSum" // byLabel
    val ascendingInCols = true
    val ascendingInRows = true
    val preColsLabels: Option[Array[String]] = None
    val colsLimitNum: Option[Int] = Some(2000)

    val aggFun = "mean" // `avg`, `max`, `min`, `sum`, `count`
    val rowAgg = true
    val colAgg = true

    val data = DataSimulate.salaryData(6000, 5, Some(1123L))
    data.show()

    import org.apache.spark.sql.functions._


    val rawData = data.select(colsLabelColName, valueColName +: rowsLabelColName: _*)
    val sortedCol = if (orderMode == "byValueSum") col("count") else col(colsLabelColName)


    import org.apache.spark.sql.udfWithNull.udf

    def contains(colsLabels: Option[Array[String]]) = udf {
      (value: Any) =>
        if (colsLabels.isEmpty)
          true
        else
          colsLabels.get.contains(value.toString)
    }

    // 此处没有直接将preColsLabels作为colLabels, 而是进行了一次统计, 目的是为了照顾到排序功能
    val colLabelsSets = rawData.filter(contains(preColsLabels)(col(colsLabelColName)))
      .groupBy(colsLabelColName)
      .agg(count(lit(1)).as("count"))
      .orderBy(if (ascendingInCols) sortedCol else sortedCol.desc)
    colLabelsSets.cache()
    colLabelsSets.count()

    colLabelsSets.show()

    val colLabels = if (colsLimitNum.isEmpty)
      try {
        colLabelsSets.collect().map {
          row =>
            row.get(0).toString
        }
      } catch {
        case e: Exception => throw new Exception(s"在获取列标签时失败, 具体信息: ${e.getMessage}")
      }
    else
      try {
        colLabelsSets.take(colsLimitNum.get).map {
          row =>
            row.get(0).toString
        }
      } catch {
        case e: Exception => throw new Exception(s"在获取列标签时失败, 具体信息: ${e.getMessage}")
      }

    require(colLabels.nonEmpty,
      "发现按照您设定的参数列标签为空, 请您查看数据列标签是否全为空或者是否您设定的列标签值不在数据中" +
        "(提示: 注意列标签值所在列的数据类型是否是String或者数值)")

    /** 一些临时的列名 */
    object tmpColName extends Serializable {
      val colsLabelColName2string: String = colsLabelColName + "_str"
      val valueColName2double: String = valueColName + "_dou"
      val colAggColName = "列汇总"
      val rowAggColName = "行汇总"
    }

    val valueType = data.schema(valueColName).dataType
    val rowLabelColsType = rowsLabelColName.map { name => name -> data.schema(name).dataType }.toMap

    val dataSet = data
      .select(
        col(colsLabelColName).cast(StringType).as(tmpColName.colsLabelColName2string) +:
          col(valueColName).cast(DoubleType).as(tmpColName.valueColName2double) +:
          rowsLabelColName.map(col): _*
      )

    val naFill = true

    println("-" * 80)
    colLabels.foreach(println)


    val selectCols =
      if (colAgg)
        rowsLabelColName.map(col) ++ colLabels.map { name => col(name).cast(valueType) } :+
          colLabels.map { name => col(name) }.reduceLeft[Column] { case (res, cl) => res + cl }
            .as(tmpColName.colAggColName)
      else
        rowsLabelColName.map(col) ++ colLabels.map { name => col(name).cast(valueType) }
    val pivotRes = if (naFill)
      dataSet.groupBy(rowsLabelColName.map(col): _*)
        .pivot(tmpColName.colsLabelColName2string, colLabels)
        .agg(Map(tmpColName.valueColName2double -> aggFun))
        .na.fill(0.0)
        .orderBy(rowsLabelColName.map(name => if (ascendingInRows) col(name) else col(name).desc): _*)
    else
      dataSet.groupBy(rowsLabelColName.map(col): _*)
        .pivot(tmpColName.colsLabelColName2string, colLabels)
        .agg(Map(tmpColName.valueColName2double -> aggFun))
        .orderBy(rowsLabelColName.map(name => if (ascendingInRows) col(name) else col(name).desc): _*)


    // 离异|               单身|               未婚|                已婚|
    // 离异|                单身|                未婚|               已婚|

    val rowAggRes = if (rowAgg) {
      val aggResDF = pivotRes.select(
        colLabels.map(name => sum(col(name)).as(name)): _*)
      val aggRes = aggResDF.collect().head
      val aggResSchema = aggResDF.schema.fields
      val uu = pivotRes.sqlContext.createDataFrame(
        pivotRes.sqlContext.sparkContext.parallelize(
          Array(Row.merge(Row.fromSeq(rowsLabelColName.map(_ => null)), aggRes))
        ),
        StructType(
          rowsLabelColName.map { name => StructField(name, StringType) } ++ aggResSchema
        )
      ).select(rowsLabelColName.map { name => col(name).cast(rowLabelColsType(name)) } ++ colLabels.map(col): _*)

      pivotRes.union(uu)
    }
    else
      pivotRes

    val res = rowAggRes.select(selectCols: _*)
    res.show(20000)


  }


  test("pivot频次统计") {





  }


  test("列数超出") {


  }


  test("列汇总和行汇总") {

  }


}
