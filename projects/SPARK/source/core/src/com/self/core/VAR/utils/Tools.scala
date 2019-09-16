package com.self.core.VAR.utils

import com.self.core.VAR.models.SlidingTime
import org.joda.time.DateTime

object Tools extends Serializable {
  def slidingTimeByNatural(timeStamp: Long, unit: String): Long = {
    val dt = new DateTime(timeStamp)
    unit match {
      case "year" => dt.yearOfCentury().roundFloorCopy().getMillis
      case "month" => dt.monthOfYear().roundFloorCopy().getMillis
      case "season" => {
        val minus_month = (dt.getMonthOfYear - 1) % 3
        dt.minusMonths(minus_month).monthOfYear().roundFloorCopy().getMillis
      }
      case "week" => dt.weekOfWeekyear().roundFloorCopy().getMillis
      case "day" => dt.dayOfYear().roundFloorCopy().getMillis
      case "hour" => dt.hourOfDay().roundFloorCopy().getMillis
      case "minute" => dt.minuteOfDay().roundFloorCopy().getMillis
      case "second" => dt.secondOfDay().roundFloorCopy().getMillis
    }
  }



}
