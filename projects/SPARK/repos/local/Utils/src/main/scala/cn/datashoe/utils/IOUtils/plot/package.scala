package cn.datashoe.utils.IOUtils

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

/**
  * 图像类的IO处理
  */
package object plot {

  /**
    * 图像类
    *
    * @param width  宽度
    * @param height 高度
    * @param pixel  像素
    * @param byRows [[pixel]]按列存储还是按行存储
    */
  class Plot(
              val width: Int,
              val height: Int,
              val pixel: Array[Color],
              var byRows: Boolean = true
            ) extends Serializable {

    def writer: PlotWriter = {
      new PlotWriter(width, height, pixel, byRows)
    }

  }


  /**
    * 图像处理的写入
    *
    * @param width  宽度
    * @param height 高度
    * @param pixel  像素
    * @param byRows [[pixel]]按列存储还是按行存储
    **/
  class PlotWriter(val width: Int,
                   val height: Int,
                   val pixel: Array[Color],
                   var byRows: Boolean = true) extends Writer {
    override protected def saveImpl(path: String): Unit = {
      val url = new File(path)

      val img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
      optionMap.getOrElse("mode", "default") match {
        case "default" =>
          if (url.exists())
            throw new IllegalArgumentException(s"您输入的路径: ${url.toURI}已经存在文件, 如果确定删除该文件可以选择覆盖模式")
        case "overwrite" =>
          if (url.exists())
            url.delete()
      }

      // x, y为image的横纵坐标位置
      for (x <- 0 until width; y <- 0 until height) {
        if (byRows) {
          val color = try {
            pixel(x * height + y)
          } catch {
            case _: Exception =>
              throw new IllegalArgumentException(
                s"从像素中获得第${x * height + y}位置时失败, 像素长度为${pixel.length}"
              )
          }

          img.setRGB(x, y, color.getRGB)
        } else {
          val color = try {
            pixel(x + y * width)
          } catch {
            case _: Exception =>
              throw new IllegalArgumentException(
                s"从像素中获得第${x + y * width}位置时失败, 像素长度为${pixel.length}"
              )
          }

          img.setRGB(x, y, color.getRGB)
        }

      }
      ImageIO.write(img, "jpg", new File(path))
    }
  }


  object PlotReader extends Serializable {

    import java.awt.Color
    import java.io.File
    import javax.imageio.ImageIO

    def readImg(path: String, byRow: Boolean = true): Plot = {
      val url = new File(path)
      if (!url.exists())
        throw new IllegalArgumentException(s"路径: ${path}不存在")

      val img = try {
        ImageIO.read(new File(path))
      } catch {
        case e: Exception => throw new Exception(s"读取过程失败: ${e.getMessage}")
      }

      val width = img.getWidth()
      val height = img.getHeight()

      val pixel = Array.tabulate(width * height) {
        index =>
          val (x, y) = if (byRow)
            (index / height, index % height)
          else
            (index % height, index / height)
          new Color(img.getRGB(x, y))
      }


      new Plot(width, height, pixel, byRow)
    }

  }


}
