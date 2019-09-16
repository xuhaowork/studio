package cn.datashoe.sparkUtils

import cn.datashoe.sparkBase.SparkAPP

object DataSimulate extends SparkAPP {
  def salaryData(nums: Int, numSlices: Int = 4, seed: Option[Long] = None) = {
    import sqlc.implicits._

    import scala.collection.mutable.ArrayBuffer

    val rd = if(seed.isEmpty) new util.Random() else new util.Random(seed.get)

    var arr: ArrayBuffer[(Int, Int, String, String, String, Int)] =
      new ArrayBuffer[(Int, Int, String, String, String, Int)]

    val profession = Array("工程师", "经理", "医生", "技术员", "工人", "老师", "律师", "学生", "自由职业")
    val marriage = Array("单身", "离异", "未婚", "已婚")
    val education = Array("小学", "初中", "高中", "大学", "研究生", "博士", "博士后")
    //id，年龄，职业，婚姻，学历，薪资
    var professionId = rd.nextInt(9)

    for (i <- 1 to nums) {
      val age = rd.nextInt(50) + 20

      val marriageId = rd.nextInt(4)
      val educationId = rd.nextInt(7)
      var salary: Int = 0

      salary = educationId match {
        case 6 => professionId = rd.nextInt(3); rd.nextInt(5000) + 10000
        case 5 => professionId = rd.nextInt(3); rd.nextInt(5000) + 9000
        case 4 => professionId = rd.nextInt(4); rd.nextInt(5000) + 7000
        case 3 => professionId = rd.nextInt(6); rd.nextInt(5000) + 5000
        case 2 => professionId = rd.nextInt(7); rd.nextInt(5000) + 3000
        case _ => professionId = rd.nextInt(7); rd.nextInt(5000) + 1000
      }
      arr += ((i, age, profession(professionId), marriage(marriageId), education(educationId), salary))
    }
    val data = sc.parallelize(arr, numSlices).toDF("id", "年龄", "职业", "婚姻", "学历", "薪资")
    data
  }

  override def run(): Unit = {



  }
}
