package org.apache.spark

/**
  *
  * Author shifeng
  * Date 2018/11/29
  * Version 1.0
  * Update 
  *
  * Desc
  *
  */
package object binary {

  type RTV = FileReadType.Value
  type CTV = FileConditionType.Value

  object FileReadType extends Enumeration {
    val DIR = Value("DIR")
    val SINGLE_FILE = Value("SINGLE_FILE")
    val MULTI_FILE = Value("MULTI_FILE")
    val CONTROL=Value("CONTROL")
    val MANUAL=Value("MANUAL")
    def valueOf(`type`: String): RTV = apply(`type`)

    def apply(`type`: String): RTV = try withName(`type`.toUpperCase) catch {
      case ex: NoSuchElementException => throw new NoSuchElementException(s"你输入的类型${`type`} 不支持,details:${ex.getMessage}")
    }

    def of(arg: RTV): Option[String] = unapply(arg)

    def unapply(arg: RTV): Option[String] = Some(arg.toString)

  }

  object FileConditionType extends Enumeration {

    val FILE_NMAE_LENGTH = Value("FILE_NMAE_LENGTH")
    val FILE_TYPE = Value("FILE_TYPE")
    val FILE_NAME = Value("FILE_NAME")
    val FILE_NAME_TIME = Value("FILE_NAME_TIME")
    val FILE_NAME_SEQUENCE = Value("FILE_NAME_SEQUENCE")
    val FILE_NAME_REGREX = Value("FILE_NAME_REGREX")
    
    def valueOf(`type`: String): CTV = apply(`type`)

    def apply(`type`: String): CTV = try withName(`type`.toUpperCase) catch {
      case ex: NoSuchElementException => throw new NoSuchElementException(s"你输入的类型${`type`} 不支持,details:${ex.getMessage}")
    }

    def of(arg: CTV): Option[String] = unapply(arg)

    def unapply(arg: CTV): Option[String] = Some(arg.toString)

  }


}
