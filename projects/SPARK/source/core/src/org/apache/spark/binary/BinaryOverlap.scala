package org.apache.spark.binary

import com.zzjz.deepinsight.basic.BaseMain
import org.apache.spark.binary.load.{BinaryFiles, Conversion}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
/**
  *   author: baiyuan
  *   date: 2018/1/18
  * */

object BinaryOverlap extends BaseMain{
    override  def  run:Unit={
        val schema = StructType(
            Seq(
                StructField("数据流位置",StringType,true)
                ,StructField("数组对象", StringType ,true)
            )
        )
        import sqlc.implicits._
        val rdd = new BinaryFiles(sc).binaryFiles("D:\\data\\binarytest",8, 1000000)
        //val  rdd=sc.binaryFiles("D:\\binarytest\\20170213190810.dat")
        //  rdd.collect().foreach(x=>x._2)
        //  val  rdd=sc.binaryFiles("D:\\binarytest")
        // val  rdd=sc.textFile("D:\\binarytest")
        // val   rdd=new  textFile(sc).textFile("D:\\binarytest")
        // rdd.saveAsObjectFile("D:\\storeTest")
        // val  rdd=sc.binaryFiles("D:\\binarytest")
      //  rdd.collect().foreach(x=>x._2.toArray())
        val  rdd2=rdd.map(x=>((x._1,x._2.getOffset(),x._2.getLength()),Conversion.bytes2hex(x._2.toArray()).slice(0,20)))


     val  rdd3=rdd2.map{case (key, value) =>
         val temp = List(key) ++ value.toSeq.toList

         Row.fromSeq(temp)}

      /*  val  df=sqlc.createDataFrame(rdd3,schema)

        df.show*/

       // rdd2.collect().foreach(x=>println(Conversion.bytes2hex(x._2).slice(0,20)))
       // rdd2.collect().foreach(x=>println(Conversion.bytes2hex(x._2).slice(0,20)))
        rdd2.collect().foreach(println)

        //rdd.collect.foreach(x=> println(Conversion.bytes2hex02(x._2.toArray()).slice(0,20)))
        // rdd.collect.foreach(x=> println(Conversion.bytes2hex02(x._2.toArray()).slice(0,20)))
        println("程序成功吗(1)？")
        // rdd.map(p => Hex.encodeHex(p._2.toArray())).flatMap(r=>r).collect().slice(0,2000).foreach(print _)
        // println(Conversion.bytes2hex02(rdd.flatMap(r=>r._2.toArray()).collect()))
        //  rdd.collect().foreach(x=>x.2_)
        println("程序成功吗(2)？")
        println(rdd2.count())

  }
}
