package com.self.core.timeTransformation.objects

import com.self.core.baseApp.myAPP

/**
  * 时间格式转换
  * ----
  * 将时间字符串、UTC时间戳、Timestamp转为对应的时间类型
  * ----
  * 参数接口:
  * ----
  * 输入类型:
  * 时间字符串
  * 时间字符串类型 (如果输入多个则分别匹配)
  * 选择时间字符串类型
  * 请输入
  * 手动输入时间字符串类型 (如果输入多个则分别匹配)
  * 自适应匹配 (分别匹配)
  * 基于现有的时间字符串类型库
  * 为时间字符串类型库添加类型后自动匹配
  * UTC时间戳
  * 单位
  * 秒
  * 毫秒
  * Timestamp
  *
  * 未有效识别时间的填充方式
  * 不填充，抛出异常
  * 以空值填充
  * 以某个时间点填充
  * 请输入时间点
  *
  * 输出类型
  * 时间字符串
  * 时间字符串类型
  * 选择时间字符串类型
  * 手动输入时间字符串类型
  * UTC时间戳
  * 单位
  * 秒
  * 毫秒
  * Timestamp
  */
object TimeFormatTransformation extends myAPP {
  override def run(): Unit = {
    import java.sql.Timestamp
    import java.text.SimpleDateFormat

    import com.google.gson.{Gson, JsonObject, JsonParser}
    import com.self.core.timeTransformation.models.UDFCreator
    import com.self.core.utils.pretreatmentUtils
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.types.{LongType, StringType, TimestampType}

    val jsonParam = "<#zzjzParam#>"

    val z1 = z
    val parser = new JsonParser()
    val pJsonParser: JsonObject = parser.parse(jsonParam).getAsJsonObject
    val rddTableName = "<#zzjzRddName#>"

    /** 1)获取DataFrame */
    val tableName = pJsonParser.get("inputTableName").getAsString
    val rawDataFrame = z1.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    try {
      rawDataFrame.schema.fieldNames.length
    } catch {
      case _: Exception => throw new Exception(s"在获得数据'$tableName'时失败，请确保您填入的是上一个结点输出的数据")
    }

    /** 2)字段处理 */
    val timeFieldName = pJsonParser.get("timeFieldName")
      .getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
    pretreatmentUtils.columnExists(timeFieldName, rawDataFrame, true)

    val timeColType = rawDataFrame.schema(timeFieldName).dataType
    val inputTimeFieldObj = pJsonParser
      .get("inputTimeFieldObj").getAsJsonObject
    inputTimeFieldObj.get("value").toString match {
      case "TimeString" =>
        require(timeColType == StringType,
          s"您选择的时间列类型为'时间字符串', 需要是string类型的时间列, 但实际该列类型为${timeColType.simpleString}")
      case "UTC" =>
        require(timeColType == LongType, s"您选择的时间列类型为'UTC时间', " +
          s"需要是Long类型的时间列, 但实际该列类型为${timeColType.simpleString}")
      case "Timestamp" =>
        require(timeColType == TimestampType, s"您选择的时间列类型为'Timestamp', " +
          s"需要是Timestamp类型的时间列, 但实际该列类型为${timeColType.simpleString}")
    }


    /**
      * 3)时间处理
      * --[解析 + 处理 + 转换]于一体
      * --UDF中尽量不用到闭包, 保证最少的结点间最少的数据传递
      */
    val naFillObj = pJsonParser.get("naFill").getAsJsonObject
    val (fillNa, fillTime) = if (naFillObj.get("value").getAsString == "byTime") (true, new Timestamp(
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(naFillObj.get("fillTime").getAsString).getTime))
    else
      (false, null)
    val transformFunction = UDFCreator.udfCreate(inputTimeFieldObj, pJsonParser, fillNa, fillTime)

    /** 4)时间字符串字段解析处理 --只有时间列为string类型时才成立 */
    val outputColName = pJsonParser.get("outputName").getAsString
    pretreatmentUtils.validNewColName(outputColName, rawDataFrame)

    /** 5)未成功解析时间的填充方式 */
    val newDataFrame = naFillObj.get("value").getAsString match {
      case "drop" =>
        rawDataFrame.na.drop(Seq(outputColName))
      case format if format == "null" || format == "fillTime" =>
        rawDataFrame.withColumn(outputColName, transformFunction(col(timeFieldName)))
    }


  }
}
