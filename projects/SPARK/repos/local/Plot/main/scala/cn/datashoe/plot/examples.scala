package cn.datashoe.plot

object examples extends App {
  def histPlot(): Unit = {
    import breeze.plot._

    val figure = Figure()

    val p1 = figure.subplot(0)

    val rd = new java.util.Random()
    p1 += hist(Array.range(0, 100).map(_ => rd.nextDouble()), 20)

    val p2 = figure.subplot(2, 1, 1)
    p2 += hist(Array.range(0, 100).map(_ => rd.nextDouble()), 10)

  }


  override def main(args: Array[String]): Unit = {
    histPlot()


    breeze.stats.distributions.Rayleigh(1)

  }

}
