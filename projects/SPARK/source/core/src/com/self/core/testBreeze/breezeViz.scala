package com.self.core.testBreeze

import com.self.core.baseApp.myAPP

object breezeViz extends myAPP {
  override def run(): Unit = {
    println("This is a test file.")
    import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, linspace}
    import breeze.plot._

    val a = new BDV[Int](1 to 3 toArray)
    val b = new BDM[Int](3, 3, 1 to 9 toArray)

    val f = Figure()
    val p = f.subplot(0)
    val x = linspace(0.0, 1.0)

    p += plot(x, x :^ 2.0)
    p += plot(x, x :^ 3.0, '.', name = "scatter")
    p.title = "scatters"
    p.panel
    p.xlabel = "x axis"
    p.ylabel = "y axis"
////    f.saveas("d:\\lines.png")
//
//    val p2 = f.subplot(2, 1, 1)
//    val g = breeze.stats.distributions.Gaussian(0, 1)
//    p2 += hist(g.sample(100000), 1000)
//    p2.title = "A normal distribution"
////    f.saveas("d:\\subplots.png")
//
//    val f2 = Figure()
//    f2.subplot(0) += image(BDM.rand(200, 200))
////    f2.saveas("d:\\image.png")

  }
}
