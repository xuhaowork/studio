package cn.datashoe.utils

import java.sql.Timestamp
import java.util.{Date, Locale, TimeZone}

import scala.util.matching.Regex

package object TimeWrangling {

  import java.text.SimpleDateFormat

  import scala.collection.mutable.{Map => muMap}

  /**
    * 时间字符串解析
    */
  object StringTimeParser extends Serializable {
    /**
      * 说明:
      * 1)这是一个常用的时间字符串的库
      * 2)为了减少误匹配的概率以及匹配的次数, 根据常用时间字符串包含的字符, 分类为四类:
      * "-"、"/"、"年|月|日"的类以及其他类(用"0"索引)
      * 3)是字符串类型的时间, 同时满足java的时间标准, 具体参考 test/resources中的对应关系
      * 4)基于该库的设计是: 先用输入的SimpleDateFormat解析, 解析不成功再用repository [ 该解析利用2)中所述的分类方法 ]
      * ----
      * 注意:
      * 序列顺序应该是按照信心量有大到小排序，时间信息越多越具体越靠前:
      * 比如"yyyy-MM-dd HH:mm:ss.SSS"要比"yyyy-MM-dd"靠前，否则"2018-01-08 18:22:03"是能够被"yyyy-MM-dd"识别的，只是识别为
      * "2018-01-08 00:00:00"
      */
    lazy val repository: muMap[Char, List[SimpleDateFormat]] = muMap(
      '-' ->
        List(
          new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.getDefault(Locale.Category.FORMAT)),
          new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.getDefault(Locale.Category.FORMAT)),
          new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSSXXX", Locale.getDefault(Locale.Category.FORMAT)),
          new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSSZ", Locale.getDefault(Locale.Category.FORMAT)),
          new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SXXX", Locale.getDefault(Locale.Category.FORMAT)),
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
          new SimpleDateFormat("yyyyMMddHHmmssSSS", Locale.getDefault(Locale.Category.FORMAT)),
          new SimpleDateFormat("yyyyMMddHHmmss", Locale.getDefault(Locale.Category.FORMAT)),
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
      * 时间字符串解析
      *
      * @param time 时间字符串
      */
    def parse(withRepository: Boolean, timeFormat: SimpleDateFormat*)(time: String): Timestamp = {
      if (withRepository) {
        val res = parse(false, timeFormat: _*)(time)
        if (res == null) {
          val pattern1 = new Regex(".*\\-.*")
          val pattern2 = new Regex(".*\\/.*")
          val pattern3 = new Regex(".*[年|月|日].*")

          time match {
            case pattern1() => parse(false, repository('-'): _*)(time)
            case pattern2() => parse(false, repository('/'): _*)(time)
            case pattern3() => parse(false, repository('年'): _*)(time)
            case _ => parse(false, repository('0'): _*)(time)
          }
        } else
          res
      } else {
        var i = 0
        var res: Timestamp = null
        while (i < timeFormat.length && res == null) {
          res = new Timestamp(timeFormat(i).parse(time).getTime)
          i += 1
        }
        res
      }
    }


  }


  /**
    * 长整型时间解析
    */
  class TimeWithLongParser() extends Serializable {
    var epochFormat: String = "utc" // fileTimeUTC
    var precision: Int = 1
    var timeUnit: String = "millisecond" // millisecond

    def setEPOCH(EPOCHFormat: String): this.type = {
      require(Array("utc", "filetimeutc") contains EPOCHFormat.toLowerCase(),
        s"长整型时间类型需要在'${Array("utc", "fileTimeUTC").mkString(", ")}'之中")
      this.epochFormat = EPOCHFormat.toLowerCase()
      this
    }

    def setPrecision(precision: Int): this.type = {
      require(precision > 0, "长整型时间的最小精度数值需要大于0")

      if (timeUnit != "second")
        require(precision < 1000, "长整型时间单位小于秒时精度不能超过1000, 超过时您可以选择更高一级的时间单位")

      this.precision = precision
      this
    }

    def setTimeUnit(timeUnit: String): this.type = {
      if (!(Array("second", "millisecond", "microsecond", "nanosecond") contains timeUnit))
        throw new IllegalArgumentException(s"目前仅支持second | millisecond | microsecond | nanosecond作为计时单位, " +
          s"您的时间单位为: '$timeUnit'")

      if (timeUnit != "second")
        require(precision < 1000, "长整型时间单位小于秒时精度不能超过1000, 超过时您可以选择更高一级的时间单位.")

      this.timeUnit = timeUnit
      this
    }

    def parser(time: Long): Timestamp = {
      // 通常情况下precision都是10的整个数倍, 且小于1000, 因此可以调换乘积顺序防止上溢
      val timeByMillis = if (1000.0 / precision == 1000 / precision) {
        println("整除")
        timeUnit match {
          case "second" => time * precision.toLong * 1000L
          case "millisecond" => time * precision.toLong
          case "microsecond" => time / (1000L / precision.toLong)
          case "nanosecond" => time / (1000000L / precision.toLong)
        }
      } else {
        // 未整除时, 顺序不能调换否则可能上溢(fileTimeUTC位数过大会上溢)
        timeUnit match {
          case "second" => time * precision.toLong * 1000L
          case "millisecond" => time * precision.toLong
          case "microsecond" => time / 1000L * precision.toLong
          case "nanosecond" => time / 1000000L * precision.toLong
        }
      }


      epochFormat.toLowerCase() match {
        case "utc" => new Timestamp(timeByMillis.toLong)
        case "filetimeutc" => new Timestamp(timeByMillis.toLong - 11644473600000L)
        case _ => throw new Exception("目前只支持utc | fileTimeUTC, 不计大小写")
      }
    }

  }

  object TimeWithLongParser extends Serializable {

    object EPOCH_FORMAT extends Serializable {
      val UTC = "utc"
      val FILE_TIME_UTC = "filetimeutc"
    }

    object TIME_UNIT extends Serializable {
      val SECOND = "second"
      val MILLISECOND = "millisecond"
      val MICROSECOND = "microsecond"
      val NANOSECOND = "nanosecond"
    }

  }


  case class StringTimeFormatter(simpleDateFormat: SimpleDateFormat) {
    def format(time: Timestamp): String = {
      simpleDateFormat.format(time)
    }

    def format(time: Date): String = {
      simpleDateFormat.format(time)
    }
  }

  class StringTimeFormatCreator() extends Serializable {
    var timezone: TimeZone = TimeZone.getDefault
    var charset: Locale = Locale.getDefault()

    var timeFormat = "yyyy-MM-dd HH:mm:ss"

    def setTimeFormat(timeFormat: String): this.type = {
      this.timeFormat = timeFormat
      this
    }

    def setCharset(charset: String): this.type = {
      this.charset = charSetByName(charset.toUpperCase())
      this
    }

    private def charSetByName(charset: String): Locale = charset match {
      case "DEFAULT" => Locale.getDefault()
      case "ENGLISH" => Locale.ENGLISH
      case "FRENCH" => Locale.FRENCH
      case "ITALIAN" => Locale.ITALIAN
      case "JAPANESE" => Locale.JAPANESE
      case "KOREAN" => Locale.KOREAN
      case "CHINESE" => Locale.CHINESE
      case "SIMPLIFIED_CHINESE" => Locale.SIMPLIFIED_CHINESE
      case "TRADITIONAL_CHINESE" => Locale.TRADITIONAL_CHINESE
      case "FRANCE" => Locale.FRANCE
      case "GERMANY" => Locale.GERMANY
      case "ITALY" => Locale.ITALY
      case "JAPAN" => Locale.JAPAN
      case "KOREA" => Locale.KOREA
      case "CHINA" => Locale.CHINA
      case "PRC" => Locale.PRC
      case "TAIWAN" => Locale.TAIWAN
      case "UK" => Locale.UK
      case "US" => Locale.US
      case "CANADA" => Locale.CANADA
      case "CANADA_FRENCH" => Locale.CANADA_FRENCH
      case _ => throw new IllegalArgumentException("目前仅支持以下国家或地区的字符集: " +
        "ENGLISH, FRENCH, ITALIAN, JAPANESE, KOREAN, CHINESE, SIMPLIFIED_CHINESE, TRADITIONAL_CHINESE, FRANCE, " +
        "GERMANY, ITALY, JAPAN, KOREA, CHINA, PRC, TAIWAN, UK, US, CANADA, CANADA_FRENCH")
    }

    /**
      * 通过时区ID设置时区
      * @param zoneId 时区ID，形如`Asia/Shanghai`, 需要在操作系统支持的时区ID中
      * @return
      */
    def setZone(zoneId: String): this.type = {
      val availableZones = TimeZone.getAvailableIDs()
      require(availableZones contains zoneId,
        s"可以通过数字设置时区(东为正, 西为负, 不设置时为默认时区), 或者通过字符串设置时间(如Asia/Shanghai), " +
          s"但您输入的时区'$zoneId'不在系统支持的时区ID中:${TimeZone.getAvailableIDs.mkString(", ")}")
      timezone = TimeZone.getTimeZone(zoneId)
      this
    }

    /**
      * 通过时区编号设定时区
      *
      * @param zone 需要在[-12, 14]之间, 正表示东, 负表示西
      *             1)超过12的时区, 为一些特殊地区的时间莱恩群岛(LINT)为东14区;
      *             2)西12区和东12区虽然是一个时区但由于日界线, 两者相差1天, 通常情况下用东12区
      * @return
      */
    def setZone(zone: Int): this.type = {
      require(zone >= -12 && zone <= 14, "时区需要在东14至西12之间, 东为正, 西为负. 备注: " +
        "1)超过12的时区, 为一些特殊地区的时间莱恩群岛(LINT)为东14区; " +
        "2)西12区和东12区虽然是一个时区但由于日界线, 两者相差1天, 通常情况下用东12区")
      val zoneId = if (zone > 0) "Etc/GMT-" + zone else "Etc/GMT+" + -zone
      timezone = TimeZone.getTimeZone(zoneId)
      this
    }

    def create(): StringTimeFormatter = {
      val formatter = new SimpleDateFormat(timeFormat, charset)
      formatter.setTimeZone(timezone)
      StringTimeFormatter(formatter)
    }

  }


}
