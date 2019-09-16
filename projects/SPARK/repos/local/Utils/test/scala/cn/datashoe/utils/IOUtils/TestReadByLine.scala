package cn.datashoe.utils.IOUtils

import scala.collection.mutable

object TestReadByLine {
  def main(args: Array[String]): Unit = {
    import scala.io.Source
    val ss = Source.fromFile("F:\\My_Workplace\\data\\LANL-Earthquake-Prediction\\train.csv").getLines()
    val arr = new mutable.Queue[String]()


    var string = ""
    var i = 0
    var flag = true
    while(ss.hasNext && flag) {
      string = ss.next()
      if(i >= 1 && string.split(",")(1).toDouble <= 0.0001)
        flag = false
      arr.enqueue(string)
      if(i >= 1000) {
        arr.dequeue()
      }

      i += 1
    }

    arr.toArray.foreach(println)
    println("i为: " + i)

//    3,1.4690998974
//    6,1.4690998963
//    6,1.4690998952
//    7,1.4690998941
//    8,1.469099893
//    8,1.4690998919
//    3,1.4690998908

//    3,9.7597965785
//    5,9.7597965774
//    5,9.7597965763
//    0,9.7597965752
//    4,9.7597965741
//    1,9.759796573
//    5,9.7597965719



  }

}
