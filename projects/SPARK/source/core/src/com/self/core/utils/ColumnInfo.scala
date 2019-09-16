package com.self.core.utils

/**
  * 一些方便平台调用的列名信息
  */

import org.apache.spark.sql.types._


/**
  * 时间列的参数信息
  * ----
  * name       时间列名
  * dataType  时间列的字段类型 包括string，timestamp，long
  * timeFormat 如果是字符串类型的字段，需要进一步的时间格式，包括"yyyy-MM-dd HH:mm:ss"等等
  *                   如果是long类型的字段，需要进一步的单位，精确到秒"second"和精确到毫秒"millisecond"
  * ----
  * 用法e.x.
  *     -- example1. TimeColInfo("time", "string", Some("yyyy-MM-dd"))
  *     -- example2. TimeColInfo("time", "long", Some("millisecond"))
  *     -- example2. TimeColInfo("time", "timestamp", None)
  */
class TimeColInfo() extends ColumnInfo{
  var timeFormat: Option[String] = None

  override def apply(newName: String, newDataType: String): this.type = {
    apply(newName, newDataType, None)
  }


  def apply(newName: String, newDataType: String, newTimeFormat: Option[String]): this.type = {
    this.name = newName
    this.dataType = newDataType
    this.timeFormat = newTimeFormat
    this
  }

  def updateTimeFormat(newDataType: String, newTimeFormat: Option[String]): this.type = {
    if(newTimeFormat.isEmpty)
      require(newDataType == "timestamp", "当您输入的信息不包含时间字符串样式或者UTC精确单位信息时，dataType只能为timestamp类型")
    else
      require(newDataType == "long" || newDataType == "string", "您输入了进一步的信息，字段类型只能为string或long")
    this.dataType = newDataType
    this.timeFormat = newTimeFormat
    this
  }

  /**
    * 当新建列名存在于Schema中时，加上一个paste字符串作为后缀直至不存在于Schema
    * @param schema
    * @param paste
    * @return
    */
  override def checkName(schema: StructType, paste: String = "_"): this.type = {
    var colName = this.name
    while(schema.fieldNames contains colName){
      colName += paste
    }
    this.name = colName
    this
  }


}


/**
  * 列名信息
  * ----
  * 跟平台上通过json获得的列名信息对应
  *
  */
class ColumnInfo() extends Serializable {
  /**
    * name     列名
    * dataType 字段类型，和平台约定的类型一致：int, string, float, double, timestamp...
    */
  var name: String = ""
  var dataType: String = ""

  def apply(newName: String , newDataType: String): this.type = {
    this.name = newName
    this.dataType = newDataType
    this
  }


  /**
    * 检查列名信息是否正确
    * @param schema 列所属的DataFrame对应的Schema
    */
  def check(schema: StructType): Unit =
    try {
      if (schema.fieldNames contains name) {
        val colType = schema.apply(name).dataType.typeName
        require(colType == dataType, s"error102: 您输入的列名${name}类型和实际不一致")
      } else {
        throw new Exception(s"error101: 您输入的列名${name}不存在")
      }
    } catch {
      case _: Exception => throw new Exception(s"error1: 算子在判断列名${name}类型中失败。")
    }

  /**
    * 将列名生成StructField
    * --提示：下一步则可以调用StructType.add()函数变更已有的Schema信息
    * --e.x. val newColInfo = new ColumnInfo(newName, newType)
    *         schema.add(newColInfo.toStructField)
    */
  def toStructField: StructField = try {
    StructField(name, DataType.fromJson(name))
  } catch {
    case failure: Exception => throw new Exception(s"error103: 生成列名${name}时错误，$failure")
  }

  def existsIn(schema: StructType): Boolean = util.Try(
    schema.apply(name).dataType.typeName.toLowerCase() == dataType
  ) getOrElse false


  /**
    * 当新建列名存在于Schema中时，加上一个paste字符串作为后缀直至不存在于Schema
    * @param schema
    * @param paste
    * @return
    */
  def checkName(schema: StructType, paste: String = "_"): this.type = {
    var colName = this.name
    while(schema.fieldNames contains colName){
      colName += paste
    }
    this.name = colName
    this
  }

  def update(newName: String, newType: String): this.type = {
    this.name = newName
    this.dataType = newType
    this
  }


}
