package com.self.core.BMJL.utils

import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
  * Created by Xuhao on 2018/1/10.
  */

/**
  * editor：Xuhao
  * data： 2018/1/10 10:00:00
  */
object SchemaUtils {
  // TODO: Move the utility methods to SQL.

  /**
    * Check whether the given schema contains a column of the required data type.
    * @param colName  column name
    * @param dataType  required column data type
    */
  def checkColumnType(
                       schema: StructType,
                       colName: String,
                       dataType: DataType,
                       msg: String = ""): Unit = {
    val actualDataType = schema(colName).dataType
    val message = if (msg != null && msg.trim.length > 0) " " + msg else ""
    require(actualDataType.equals(dataType),
      s"Column $colName must be of type $dataType but was actually $actualDataType.$message")
  }

  /**
    * Appends a new column to the input schema. This fails if the given output column already exists.
    * @param schema input schema
    * @param colName new column name. If this column name is an empty string "", this method returns
    *                the input schema unchanged. This allows users to disable output columns.
    * @param dataType new column data type
    * @return new schema with the input column appended
    */
  def appendColumn(
                    schema: StructType,
                    colName: String,
                    dataType: DataType): StructType = {
    if (colName.isEmpty) return schema
    val fieldNames = schema.fieldNames
    require(!fieldNames.contains(colName), s"Column $colName already exists.")
    val outputFields = schema.fields :+ StructField(colName, dataType, nullable = false)
    StructType(outputFields)
  }

  /**
    * Appends a new column to the input schema. This fails if the given output column already exists.
    * @param schema input schema
    * @param col New column schema
    * @return new schema with the input column appended
    */
  def appendColumn(schema: StructType, col: StructField): StructType = {
    require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
  }

}
