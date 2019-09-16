package com.self.core.timeBinning.models

case class BinningPoint(point: Long, pointBackUp: Option[Long] = None) {
}
case class BinningStorage(binningPoint: BinningPoint, intervalBackUp: Option[BinningPoint] = None)