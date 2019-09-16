package com.self.core.VAR.models

import com.self.core.VAR.models.IntervalInfo

/**
  * 如果是fixedLength需要输入长度信息
  * 如果是naturalLength需要输入自然时间单位
  */
class FrequencyInfo(var intervalInfo: Option[IntervalInfo],
                    val intervalSlidingUnit: Option[String],
                    val frequencyType: String,
                    val fixedLength: Option[Long],
                    val naturalUnit: Option[String]){
  def updateInterval(newIntervalInfo: IntervalInfo): this.type = {
    this.intervalInfo = Some(newIntervalInfo)
    this
  }
}
