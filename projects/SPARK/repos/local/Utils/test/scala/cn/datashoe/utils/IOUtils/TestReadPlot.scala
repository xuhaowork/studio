package cn.datashoe.utils.IOUtils

import java.awt.Color

import cn.datashoe.utils.IOUtils.plot.{Plot, PlotReader}
import cn.datashoe.utils.properties.SystemProperties
import org.scalatest.FunSuite
import smile.clustering.KMeans

class TestReadPlot extends FunSuite {
  test("测试") {
    println(SystemProperties.MY_WORKPLACE_SPARK)
    val basePath = s"${SystemProperties.MY_WORKPLACE_SPARK}\\projects\\SPARK\\repos\\local\\Utils\\test\\resources\\"
    val clusterCnt = 3
    val plot = PlotReader.readImg(basePath + "road.jpg")
    val data: Array[Array[Double]] = plot.pixel.map(rgb => Array(rgb.getRed.toDouble, rgb.getGreen.toDouble, rgb.getBlue.toDouble))

    val model = new KMeans(data, clusterCnt, 200)

    val deep = 256 / clusterCnt
    val label = model.getClusterLabel.map {
      i =>
        new Color(i * deep, i * deep, i * deep)
    }

    new Plot(plot.width, plot.height, label).writer.option("mode", "overwrite").save(basePath + "result.jpg")

  }


}
