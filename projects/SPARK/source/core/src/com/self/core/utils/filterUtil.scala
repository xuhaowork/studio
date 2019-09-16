/**
  * editor ：ranchao
  * date : 2017.12.01 10:00:00
  */
package com.self.core.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object filterUtil {

  /**
    * 过滤非数值行
    */
  def filterNonNumericalRow(data: DataFrame): DataFrame = {
    val filteredRdd = data.rdd.filter { row =>
      var isValid: Boolean = true
      var i = 0
      while (isValid && i < row.length) {
        try {
          row(i).toString.toDouble
        } catch {
          case _: Throwable => isValid = false
        }
        i += 1
      }
      isValid
    }
    val schema = data.schema
    data.sqlContext.createDataFrame(filteredRdd, schema)
  }

  /**
    * 过滤非数值行
    */
  def filterNonNumericalRow(data: RDD[Row]): RDD[Row] = {
    val result = data.filter { row =>
      var isValid: Boolean = true
      var i = 0
      while (isValid && i < row.length) {
        try {
          row(i).toString.toDouble
        } catch {
          case _: Throwable => isValid = false
        }
        i += 1
      }
      isValid
    }
    result
  }

  /**
    * 过滤非数值行
    */
  def filterNonNumericalRow(data: DataFrame, ids: Array[Int]): DataFrame = {
    val filteredRdd = data.rdd.filter { row =>
      var isValid: Boolean = true
      var i = 0
      while (isValid && i < ids.length) {
        try {
          row(ids(i)).toString.toDouble
        } catch {
          case _: Throwable => isValid = false
        }
        i += 1
      }
      isValid
    }
    val schema = data.schema
    data.sqlContext.createDataFrame(filteredRdd, schema)
  }

  /**
    * 过滤非数值行
    */
  def filterNonNumericalRow(data: RDD[Row], ids: Array[Int]): RDD[Row] = {
    val result = data.filter { row =>
      var isValid: Boolean = true
      var i = 0
      while (isValid && i < ids.length) {
        try {
          row(ids(i)).toString.toDouble
        } catch {
          case _: Throwable => isValid = false
        }
        i += 1
      }
      isValid
    }
    result
  }

  /**
    * 检验是否含有非数值型数据
    */
  def validTest(data: DataFrame): Boolean = {
    val invalidRdd = data.rdd.filter { row =>
      var isValid: Boolean = true
      var i = 0
      while (isValid && i < row.length) {
        try {
          row(i).toString.toDouble
        } catch {
          case _: Throwable => isValid = false
        }
        i += 1
      }
      !isValid
    }
    invalidRdd.count() == 0
  }

}

// date : 2018.01.12 10:00:00