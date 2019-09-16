package cn.datashoe.dataWrangling

import cn.datashoe.sparkBase.SparkAPP
import cn.datashoe.sparkUtils.DataSimulate


object Pivot extends SparkAPP {
  override def run(): Unit = {
    //
    val data = DataSimulate.salaryData(6000, 5, Some(1123L))
    import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

    val conf = sc.hadoopConfiguration
    val hdfs: FileSystem = FileSystem.get(conf)

    val savePath = "G:\\aa"
    val path = new Path(savePath)

    if (hdfs.exists(path)) {
      if (hdfs.isDirectory(path))
        hdfs.delete(path, true)
      else hdfs.delete(path, false)
    }
    if (hdfs.exists(path)) {
      if (hdfs.isDirectory(path))
        hdfs.delete(path, true)
      else hdfs.delete(path, false)
    }

    val split = ","
    val header = "true"
    val newPath = new Path("G:\\sss.csv")
//    org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
    data.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", header)
      .option("delimiter", split)
      .save(path.toString)
    FileUtil.copyMerge(path.getFileSystem(conf), path, newPath.getFileSystem(conf), newPath, false, conf, null)


//    import org.apache.poi.xssf.usermodel.{XSSFFormulaEvaluator, XSSFRow, XSSFSheet, XSSFWorkbook}


  }
}



import com.google.gson.JsonParser
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

object Pivot2 extends SparkAPP {
  override def run(): Unit = {


  }
}

