package com.self.core.timeTransformation.models

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.collection.mutable.{Map => muMap}

class StringTimeFormatMatcher() extends Serializable {
  /**
    * 时间解析中用到的时间字符串格式
    * ----
    * 正向是归类，分别是包含"-"、"/"、"年"的类以及其他类(用"0"索引), 这样做得目的是为了增加效率
    * ----
    * 注意序列顺序应该是按照信心量有大到小排序，时间信息越多越具体越靠前，
    * 比如"yyyy-MM-dd HH:mm:ss.SSS"要比"yyyy-MM-dd"靠前，否则"2018-01-08 18:22:03"是能够被"yyyy-MM-dd"识别的，
    * 只是识别为"2018-01-08 00:00:00"
    */
  var timeFormat: muMap[Char, List[SimpleDateFormat]] = muMap(
    '-' ->
      List(
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSXXX", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ssXXX", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd EEE HH:mm:ss.SSS", Locale.ENGLISH),
        new SimpleDateFormat("yyyy-MM-dd EEE HH:mm:ss.SSS", Locale.CHINA),
        new SimpleDateFormat("yyyy-MM-ddEEE HH:mm:ss.SSS", Locale.CHINA),
        new SimpleDateFormat("yyyy-MM-ddEEE HH:mm:ss.SSS", Locale.ENGLISH),
        new SimpleDateFormat("yyyy-MM-dd EEE HH:mm:ss", Locale.CHINA),
        new SimpleDateFormat("yyyy-MM-dd EEE HH:mm:ss", Locale.ENGLISH),
        new SimpleDateFormat("yyyy-MM-ddEEE HH:mm:ss", Locale.CHINA),
        new SimpleDateFormat("yyyy-MM-ddEEE HH:mm:ss", Locale.ENGLISH),
        new SimpleDateFormat("yyyy-MM-ddEEE", Locale.CHINA),
        new SimpleDateFormat("yyyy-MM-ddEEE", Locale.ENGLISH),
        new SimpleDateFormat("yyyy-MM-dd EEE", Locale.CHINA),
        new SimpleDateFormat("yyyy-MM-dd EEE", Locale.ENGLISH),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT))
      ),
    '/' ->
      List(
        new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss.SSSXXX", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss.SSSZ", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ssXXX", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ssZ", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSSXXX", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSSZ", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy/MM/dd HH:mm:ssXXX", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy/MM/dd HH:mm:ssZ", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy/MM/ddEEE HH:mm:ss.SSS", Locale.CHINA),
        new SimpleDateFormat("yyyy/MM/ddEEE HH:mm:ss.SSS", Locale.ENGLISH),
        new SimpleDateFormat("yyyy/MM/dd EEE HH:mm:ss.SSS", Locale.CHINA),
        new SimpleDateFormat("yyyy/MM/dd EEE HH:mm:ss.SSS", Locale.ENGLISH),
        new SimpleDateFormat("yyyy/MM/ddEEE HH:mm:ss", Locale.CHINA),
        new SimpleDateFormat("yyyy/MM/ddEEE HH:mm:ss", Locale.ENGLISH),
        new SimpleDateFormat("yyyy/MM/dd EEE HH:mm:ss", Locale.CHINA),
        new SimpleDateFormat("yyyy/MM/dd EEE HH:mm:ss", Locale.ENGLISH),
        new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy/MM/dd", Locale.getDefault(Locale.Category.FORMAT)),
        new SimpleDateFormat("yyyy/MMM/dd", Locale.CHINA),
        new SimpleDateFormat("yyyy/MMM/dd", Locale.ENGLISH),
        new SimpleDateFormat("yyyy/MMM/dd HH:mm:ss", Locale.CHINA),
        new SimpleDateFormat("yyyy/MMM/dd HH:mm:ss", Locale.ENGLISH),
        new SimpleDateFormat("MM/dd/yyyy KK:mm:ss aa", Locale.getDefault(Locale.Category.FORMAT))
      ),
    '年' ->
      List(
        new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒", Locale.CHINA),
        new SimpleDateFormat("yyyy年MM月dd日EEE HH时mm分ss秒", Locale.CHINA),
        new SimpleDateFormat("yyyy年MM月dd日 EEE HH时mm分ss秒", Locale.CHINA),
        new SimpleDateFormat("yyyy年MM月dd日EEE", Locale.CHINA),
        new SimpleDateFormat("yyyy年MM月dd日 EEE", Locale.CHINA),
        new SimpleDateFormat("yyyy年MM月dd日", Locale.CHINA)
      ),
    '0' ->
      List(
        new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss 'GMT'XXX", Locale.CHINA),
        new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss 'GMT'XXX", Locale.US),
        new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss 'GMT'Z", Locale.CHINA),
        new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss 'GMT'Z", Locale.US),
        new SimpleDateFormat("EEE dd MMM yyyy HH:mm:ss 'GMT'XXX", Locale.CHINA),
        new SimpleDateFormat("EEE dd MMM yyyy HH:mm:ss 'GMT'XXX", Locale.US),
        new SimpleDateFormat("EEE dd MMM yyyy HH:mm:ss 'GMT'Z", Locale.CHINA),
        new SimpleDateFormat("EEE dd MMM yyyy HH:mm:ss 'GMT'Z", Locale.US),
        new SimpleDateFormat("MMM dd,yyyy KK:mm:ss aa", Locale.CHINA),
        new SimpleDateFormat("MMM dd,yyyy KK:mm:ss aa", Locale.ENGLISH),
        new SimpleDateFormat("MMM dd,yyyy HH:mm:ss", Locale.CHINA),
        new SimpleDateFormat("MMM dd,yyyy HH:mm:ss", Locale.ENGLISH),
        new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.CHINA),
        new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH),
        new SimpleDateFormat("EEE dd MMM HH:mm:ss z yyyy", Locale.CHINA),
        new SimpleDateFormat("EEE dd MMM HH:mm:ss z yyyy", Locale.ENGLISH),
        new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zXXX", Locale.CHINA),
        new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zXXX", Locale.ENGLISH),
        new SimpleDateFormat("EEE dd MMM yyyy HH:mm:ss zXXX", Locale.CHINA),
        new SimpleDateFormat("EEE dd MMM yyyy HH:mm:ss zXXX", Locale.ENGLISH),
        new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss z", Locale.CHINA),
        new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss z", Locale.ENGLISH),
        new SimpleDateFormat("EEE dd MMM yyyy HH:mm:ss z", Locale.CHINA),
        new SimpleDateFormat("EEE dd MMM yyyy HH:mm:ss z", Locale.ENGLISH),
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.CHINA),
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH),
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH)
      )
  )

  /**
    * 为时间字符串解析器添加时间字符串模式
    * ----
    *
    * @param stringTimeFormat 时间字符串模式 --例如：yyyy-MM-ddEEE Z
    * @param charSet          字符集 CHINA |  ENGLISH | default
    * @return this
    */
  def appendTimeFormat(stringTimeFormat: String, charSet: String): this.type = {
    charSet match {
      case "CHINA" =>
        stringTimeFormat match {
          case time if time.toCharArray contains '-' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.CHINA)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('-' -> (List(tf) ++ timeFormat('-')))

          case time if time.toCharArray contains '/' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.CHINA)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('/' -> (List(tf) ++ timeFormat('/')))

          case time if time.toCharArray contains '年' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.CHINA)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('年' -> (List(tf) ++ timeFormat('年')))

          case time if time.toCharArray contains '月' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.CHINA)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('年' -> (List(tf) ++ timeFormat('年')))

          case _ =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.CHINA)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('0' -> (List(tf) ++ timeFormat('0')))

        }

      case "ENGLISH" =>
        stringTimeFormat match {
          case time if time.toCharArray contains '-' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.ENGLISH)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('-' -> (List(tf) ++ timeFormat('-')))

          case time if time.toCharArray contains '/' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.ENGLISH)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('/' -> (List(tf) ++ timeFormat('/')))

          case time if time.toCharArray contains '年' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.ENGLISH)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('年' -> (List(tf) ++ timeFormat('年')))

          case time if time.toCharArray contains '月' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.ENGLISH)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('年' -> (List(tf) ++ timeFormat('年')))

          case _ =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.ENGLISH)
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('0' -> (List(tf) ++ timeFormat('0')))

        }

      case "default" =>
        stringTimeFormat match {
          case time if time.toCharArray contains '-' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.getDefault(Locale.Category.FORMAT))
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('-' -> (List(tf) ++ timeFormat('-')))

          case time if time.toCharArray contains '/' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.getDefault(Locale.Category.FORMAT))
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('/' -> (List(tf) ++ timeFormat('/')))

          case time if time.toCharArray contains '年' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.getDefault(Locale.Category.FORMAT))
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('年' -> (List(tf) ++ timeFormat('年')))

          case time if time.toCharArray contains '月' =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.getDefault(Locale.Category.FORMAT))
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('年' -> (List(tf) ++ timeFormat('年')))

          case _ =>
            val tf = try {
              new SimpleDateFormat(stringTimeFormat, Locale.getDefault(Locale.Category.FORMAT))
            } catch {
              case e: Exception => throw new Exception(
                s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
              )
            }
            timeFormat += ('0' -> (List(tf) ++ timeFormat('0')))

        }


    }
    this
  }

  def appendTimeFormat(stringTimeFormat: Seq[String]): this.type = {
    stringTimeFormat.foreach {
      sdf =>
        appendTimeFormat(sdf, "CHINA")
        appendTimeFormat(sdf, "ENGLISH")
    }
    updateRepoOrder()
    this
  }

  def updateTimeFormat(stringTimeFormat: Seq[String]): this.type = {
    val stf = stringTimeFormat.map {
      stringTimeFormat =>
        try {
          new SimpleDateFormat(stringTimeFormat, Locale.getDefault(Locale.Category.FORMAT))
        } catch {
          case e: Exception => throw new Exception(
            s"您输入的时间字符串格式'$stringTimeFormat'未能转换为时间字符串解析器，具体异常为:${e.getMessage}"
          )
        }
    }

    timeFormat = muMap('s' -> stf.toList)
    updateRepoOrder()
    this
  }

  private def updateRepoOrder(): this.type = {
    timeFormat.foreach {
      case (key, values) =>
        timeFormat += (key -> values.sortBy(sdf => -sdf.format(new Date(0L)).length))
    }
    println(timeFormat.mapValues(sdf => sdf.map(sdf => sdf.format(new Date(0L))).mkString(",")))
    this
  }

  /**
    * 基于时间字符串模式分类进行解析
    *
    * @param category 类别 --'s'表示客户
    * @param time     时间
    * @return 如果解析成功返回时间戳和匹配的时间字符串样式, 否则返回(null, null)
    */
  private def parseByCategory(category: Char)(time: String): (Timestamp, SimpleDateFormat) = {
    var res: Date = null
    var matchPattern: SimpleDateFormat = null
    var flag = false
    if (timeFormat.contains(category)) {
      val timeFormats = timeFormat(category).toIterator
      while (!flag && timeFormats.hasNext) {
        val timeFormat = timeFormats.next()
        try {
          res = timeFormat.parse(time)
          matchPattern = timeFormat
          if(res != null){
            println(res, timeFormat)
            flag = true
          }
        } catch {
          case _: Exception =>
        }
        println(flag, category)
      }
    }

    if (res != null) (new Timestamp(res.getTime), matchPattern) else (null, null)
  }

  /**
    * 自适应解析时间字符串的格式
    *
    * @param string 时间字符串
    * @return 解析的Date格式和与之匹配的SimpleDateFormat
    */
  def parseFormat(string: String): (Timestamp, SimpleDateFormat) = {
    val (ts, sdf) = parseByCategory('s')(string)
    val res = if (ts == null) {
      string match {
        case time if time.toCharArray contains '-' =>
          parseByCategory('-')(time)
        case time if time.toCharArray contains '/' =>
          parseByCategory('/')(time)
        case time if time.toCharArray contains '年' =>
          parseByCategory('年')(time) // todo: 年应该放在0中
        case _ =>
          (null, null)
      }
    } else
      (ts, sdf)

    if (res._1 == null) {
      parseByCategory('0')(string)
    } else
      (res._1, res._2)
  }

  /**
    * 自适应解析时间字符串的格式
    *
    * @param string 时间字符串
    * @return 解析的Date
    */
  def parse(string: String): Timestamp = parseFormat(string: String)._1


}
