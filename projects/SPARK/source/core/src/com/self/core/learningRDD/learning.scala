package com.self.core.learningRDD

import com.self.core.baseApp.myAPP

object learning extends myAPP {
  def test() = {

    import org.apache.spark.rdd.RDD

    val z1 = outputrdd
    val rdd = z1.get("流输出_1_ZyqIgqXY_rdd").asInstanceOf[RDD[String]]
    val u = rdd.collect()
//      .foreach(println)


  }


  override def run(): Unit = {

    test()




    //    /** 模拟数据 */
    //    val rd = new java.util.Random(123L)
    //
    //    // 三个中心
    //    val clusters = Array(Array(1.1, 2.0, 3.5), Array(-1.5, 2.0, -3.5), Array(0.0, 0.0, 0.0))
    //
    //    import org.apache.spark.mllib.linalg.Vectors
    //    val data = sc.parallelize(clusters.zipWithIndex.flatMap {
    //      case (arr, idx) => Array.tabulate(1000)(_ => (Vectors.dense(arr.map(_ + rd.nextGaussian())), idx))
    //    })
    //
    //
    //    val data2 = data.map{case (_, idx) => idx}
    //    data2.cache() // 没有action操作不会缓存数据
    //
    ////    data2.count() // 此时CacheManager触发cache数据
    //
    //    val data3 = data.map{case (vec, _) => vec}
    //
    //
    //
    //    val a: Array[Product with Serializable] = Array(Tuple1(1), Tuple2(2, 3))


    //    import org.apache.spark.{Partitioner, SparkException}
    //    import org.apache.spark.rdd.RDD
    //
    //    /**  重分区解决交叠分区问题 */
    //    /** 每个分区若干个元素, 总共10000000个元素, 分在10个分区中 */
    //    val testNum = 1000
    ////    0000
    //    val partitions = 10
    //    val rd = new java.util.Random(123L)
    //    val dt = Seq.fill(testNum)(rd.nextGaussian())
    //
    //    val rawRdd: RDD[Double] = sc.parallelize(dt, partitions)
    //    rawRdd.cache()
    //    rawRdd.count()
    ////    rawRdd.foreachPartition(arr =>
    ////      println(arr.mkString(", "))
    ////    )
    //
    //
    //    println()
    //    println()
    //    println("-"*30 + "分割线" + "-"*30)
    //    println()
    //    println()
    //
    //
    //    def testPartition() = {
    //      import org.apache.spark.{Partitioner, SparkException}
    //      import org.apache.spark.rdd.RDD
    //      val rdd = sc.parallelize(Seq(
    //        "A","B","C","D","E","F","G",
    //        "H","I","J","K","L","M","N",
    //        "O","P","Q","R","S","T",
    //        "U","V","W","X","Y","Z"
    //      ), 4)
    //
    //
    //      val res1 = rdd.mapPartitionsWithIndex{(index, iter) => Array((index, iter.toArray)).toIterator}.reduceByKey(_ ++ _)
    //        res1.collectAsMap().foreach(iter => println(iter._1, iter._2.mkString(",")))
    //
    //      println("-"*80)
    //
    //      val partitionMaterial: RDD[(Long, String)] = rdd.mapPartitionsWithIndex {
    //        (index, iter) => {
    //          val partitionNum = index.toLong << 33
    //          var i = 0
    //          val result = mutable.ArrayBuilder.make[(Long, String)]()
    //          iter.foreach {
    //            value => {
    //              i += 1
    //              if (index != 0 && i == 1) {
    //                result += Tuple2(partitionNum - 1, value)
    //                result += Tuple2(partitionNum, value)
    //              } else {
    //                result += Tuple2(partitionNum + i - 1, value)
    //              }
    //            }
    //          }
    //          result.result().toIterator
    //        }
    //      }
    //
    //
    //      class OverLapPartitioner(numParts: Int) extends Partitioner {
    //        override def numPartitions: Int = numParts
    //
    //        override def getPartition(key: Any): Int = {
    //          val id = key match {
    //            case i: Long =>
    //              i >> 33
    //            case _: Exception =>
    //              throw new SparkException("key的类型不是long")
    //          }
    //          val modNum = (id % numParts).toInt
    //          if (modNum < 0) modNum + numParts else modNum
    //        }
    //      }
    //
    //      val rdd2 = partitionMaterial.partitionBy(new OverLapPartitioner(4))
    //
    //      sc.getConf.get("")
    //
    //
    ////        .map(_._2)
    //      rdd2.mapPartitionsWithIndex {
    //        case (index, iter) =>
    //          iter.map(v => (index, v))
    //      }.collect().sortBy(_._1).foreach(println)
    //
    //      rdd2.mapPartitionsWithIndex {
    //        case (index, iter) =>
    //          iter.toArray.head
    //          Iterator((index, Some(iter.toArray.head)))
    //      }.collect().sortBy(_._1).foreach(println)
    //
    //
    //
    //      val rdd3 = partitionMaterial.map { case (key, value) => (key % 4, value)}.groupByKey()
    //        .mapValues(iter => iter.mkString(","))
    //
    //      rdd3.collect().sortBy(_._1).foreach(println)


    /*      val res2 = rdd2.mapPartitionsWithIndex{(index, iter) => Array((index, iter.toArray)).toIterator}.reduceByKey(_ ++ _)
            res2.collectAsMap().foreach(iter => println(iter._1, iter._2.mkString(",")))*/

    //      (2,N,O,P,Q,R,S)
    //      (1,G,H,I,J,K,L,M)
    //      (3,T,U,V,W,X,Y,Z)
    //      (0,A,B,C,D,E,F)
    /** 执行第一次 */
    //      --------------------------------------------------------------------------------
    //      (2,N,O,P,Q,R,S,T)
    //      (1,N,G,H,I,J,K,L,M)
    //      (3,T,U,V,W,X,Y,Z)
    //      (0,G,A,B,C,D,E,F)


    //      (2,N,O,P,Q,R,S,T)
    //      (1,G,H,I,J,K,L,M,N)
    //      (3,T,U,V,W,X,Y,Z)
    //      (0,G,A,B,C,D,E,F)

    //      (2,T,N,O,P,Q,R,S)
    //      (1,G,H,I,J,K,L,M,N)
    //      (3,T,U,V,W,X,Y,Z)
    //      (0,G,A,B,C,D,E,F)

    //    }

    //    println("-"*30 + "分割线111" + "-"*30)
    //
    //    testPartition()
    //
    //
    //    sc.parallelize(Seq.range(0, 100), 10).map(i => i * 10).map(i => i + 1).reduce(_ + _)


    /**
      * 假设进行的是x和lag_1(x)之间的乘积运算,
      * 此时
      * 1)第i个分区的x需要第i+1个分区的第一个元素firstFromNextPartition
      * 2)x.drop(1)::firstFromNextPartition zip x
      * 3)最后再求和
      * ----
      * 通过分区搞定第一步————交叠分区:
      * 1)先给分区加上key同时除了第一个分区外，每个分区的第一个元素都变成双份
      * 2)创建一个Partitioner定义一个恰当的规则拉取下一个分区的第一个元素
      *
      * 第1)步的key定义和第2)步的Partitioner定义是连在一起的, 这里算法设计如下:
      * 1)key定义
      * 0-0  1 << 33 + 0
      * 0-1  1 << 33 + 1
      * ...
      * 0-52 1 << 33 + 52
      * 1-0  2 << 33 - 1
      * 1-0  2 << 33 + 0
      * 1-1  2 << 33 + 1
      * ...
      * 1-50 2 << 33 + 50
      * 2-0  3 << 33 - 1
      * 2-0  3 << 33 + 0
      * 2-1  3 << 33 + 1
      * ...
      * ...
      * 2)分区规则
      * id = (key >> 33) - 1
      * 然后根据分区数对id取非负模运算
      *
      */

    //    def test1():Long = {
    //      val startTime1 = System.nanoTime()
    //      /** 1)通过map映射一个合理的key */
    //      val partitionMaterial: RDD[(Long, Double)] = rawRdd.mapPartitionsWithIndex {
    //        (index, iter) => {
    //          val partitionNum = (index.toLong + 1) << 33
    //          var i = 0
    //          val result = mutable.ArrayBuilder.make[(Long, Double)]()
    //          iter.foreach {
    //            value => {
    //              i += 1
    //              if (index != 0 && i == 1) {
    //                result += Tuple2(partitionNum - 1, value)
    //                result += Tuple2(partitionNum, value)
    //              } else {
    //                result += Tuple2(partitionNum + i - 1, value)
    //              }
    //            }
    //          }
    //          result.result().toIterator
    //        }
    //      }
    //
    //
    //      class OverLapPartitioner(numParts: Int) extends Partitioner {
    //        override def numPartitions: Int = numParts
    //
    //        override def getPartition(key: Any): Int = {
    //          val id = key match {
    //            case i: Long =>
    //              (i >> 33) - 1
    //            case _: Exception =>
    //              throw new SparkException("key的类型不是long")
    //          }
    //          val modNum = (id % numParts).toInt
    //          if (modNum < 0) modNum + numParts else modNum
    //        }
    //      }
    //
    //      val rePartitionRdd = partitionMaterial.partitionBy(new OverLapPartitioner(4))
    //      val sum = rePartitionRdd.map(_._2).mapPartitions(
    //        iter => {
    //          var result = 0.0
    //          var lastValue = 1.0
    //          while (iter.hasNext) {
    //            val value = iter.next()
    //            result += value * lastValue
    //            lastValue = value
    //          }
    //          Array(result).toIterator
    //        }
    //      ).reduce(_ + _)
    //      println("最终结果:" + sum)
    //
    //      val endTime1 = System.nanoTime()
    //      val costTime1 = (endTime1 - startTime1) / 1000
    //      println(s"花费时间:${costTime1}毫秒")
    //      costTime1
    //    }
    //
    ////    println()
    ////    println()
    ////    println("-"*30 + "分割线" + "-"*30)
    ////    println()
    ////    println()
    //
    //
    //    def test2(): Long = {
    //      val startTime2 = System.nanoTime()
    //      val zipRdd = rawRdd.zipWithIndex().map { case (value, index) => (index, value) }
    //      val joinRdd = zipRdd.map { case (index, value) => (index - 1, value) }
    //      val sum2 = zipRdd.join(joinRdd).map { case (_, d2) => d2._1 * d2._2 }.reduce(_ + _)
    //      println("最终结果:" + sum2)
    //      val endTime2 = System.nanoTime()
    //      val costTime2 = (endTime2 - startTime2) / 1000
    //      println(s"花费时间:${costTime2}毫秒")
    //      costTime2
    //    }


    // 本地测试一次1000数据
    //    最终结果:47.05076687713064
    //    花费时间:706524毫秒
    //    ------------------------------分割线------------------------------
    //    最终结果:50.25221589195291
    //    花费时间:1877707毫秒


    // 平台测试一次
    //    最终结果:3073.746162314362
    //    花费时间:11705075毫秒
    //      ------------------------------分割线------------------------------
    //    最终结果:3075.894099593756
    //    花费时间:12812115毫秒


    //    // 平台测试10次
    //    var time1 = 0L
    //    var time2 = 0L
    //    var i = 0
    //    while (i < 10) {
    //      time1 += test1()
    //      time2 += test2()
    //      i += 1
    //    }
    //
    //    println("-"*100)
    //    println("time1:" + time1)
    //    println("time2:" + time2)


    //    最终结果:3065.6536582141607
    //    花费时间:6226275毫秒
    //      最终结果:3075.8940995937674
    //    花费时间:30172220毫秒
    //      最终结果:3071.0553913042263
    //    花费时间:6301985毫秒
    //      最终结果:3075.8940995937646
    //    花费时间:30000736毫秒
    //      最终结果:3070.9924998781403
    //    花费时间:7523634毫秒
    //      最终结果:3075.894099593764
    //    花费时间:10263537毫秒
    //      最终结果1:3066.600221317917
    //    花费时间:10315701毫秒
    //      最终结果:3075.894099593765
    //    花费时间:9150718毫秒
    //      最终结果1:3069.533905151881
    //    花费时间:7890672毫秒
    //      最终结果:3075.8940995937696
    //    花费时间:11433651毫秒
    //      最终结果:3067.7177049532374
    //    花费时间:6847593毫秒
    //      最终结果:3075.8940995937655
    //    花费时间:9449385毫秒
    //      最终结果:3066.4423916323813
    //    花费时间:7317898毫秒
    //      最终结果:3075.8940995937655
    //    花费时间:16365715毫秒
    //      最终结果:3067.9931855052337
    //    花费时间:8651666毫秒
    //      最终结果:3075.894099593772
    //    花费时间:15467284毫秒
    //      最终结果:3066.647921527847
    //    花费时间:6839846毫秒
    //      最终结果:3075.8940995937705
    //    花费时间:8934842毫秒
    //      最终结果:3066.4449757607445
    //    花费时间:6121397毫秒
    //      最终结果:3075.8940995937623
    //    花费时间:14859529毫秒
    //      ----------------------------------------------------------------------------------------------------
    //    time1:74036667
    //    time2:156097617
    //    最终结论：交叠分区要比join快，但交叠分区可能的分区值是不同的，结果居然不同，这个问题还有待解决。

    //    import org.apache.spark.sql.functions
    //
    //
    //    import com.google.gson.{Gson, JsonParser}
    //    import org.apache.spark.sql.DataFrame
    //    import org.apache.spark.sql.functions.{col, udf}
    //    import org.apache.spark.sql.types.StringType
    //    import org.apache.spark.sql.NullableFunctions
    //
    //    val jsonparam = """{"RERUNNING":{"nodeName":"通用格式转换_1","preNodes":[{"checked":true,"id":"文本目录数据源新版升级_1_wvzrwHW4"}],"rerun":"false"},"changeInforObj":{"changeInfor":".","value":"false"},"invalidChars":"\"","tableName":"文本目录数据源新版升级_1_wvzrwHW4"}"""
    //    println(jsonparam)
    //    val gson = new Gson()
    //    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    //    val tableName = p.get("tableName")
    //
    //
    //    val parser = new JsonParser()
    //    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
    //
    //
    //    val changeInforFormat = pJsonParser.getAsJsonObject("changeInforObj").get("value").getAsString
    //    val invalidChars = p.get("invalidChars")
    //    var changeInfor = ""
    //    if (changeInforFormat == "false") {
    //      changeInfor = pJsonParser.getAsJsonObject("changeInforObj").get("changeInfor").getAsString //请输入替代的字符串
    //    }
    //
    //
    //    val regexArray = invalidChars.split(";")
    //
    //    val z1 = outputrdd
    //    var inputDf: DataFrame =null
    //
    //    var i = 1
    //    val schema: StructType = null
    //
    //    val specialStrArr: Array[String] = Array("+", ".", "*", "$", "^", "?", "[", "{", "(", ")", "|", "\\", ".")
    //    val func = (str: String) => {
    //      val strTemp1 = if (str == null) "" else str
    //
    //      val newStrTemp = if (changeInfor == null) "" else changeInfor
    //      val strTemp = if (i == 1) {
    //        val head = strTemp1.getBytes()
    //        if (head(0) == (-17) && head(1) == (-69) && head(2) == (-65)) head.drop(3).map(_.toChar).mkString else strTemp1
    //      } else strTemp1
    //      i = i + 1
    //
    //      val headStr = util.Try(str.head.toString).getOrElse("")
    //      val lastStr = util.Try(str.last.toString).getOrElse("")
    //      val tail = if(str.isEmpty) "" else if (specialStrArr.contains(lastStr)) s"\\${str.last}" else lastStr //${} 字符串占位符
    //      val head =if(str.isEmpty) "" else if (specialStrArr.contains(headStr)) s"\\${str.head}" else headStr
    //      val result = if (regexArray.contains(headStr) && regexArray.contains(lastStr))
    //        strTemp.replaceFirst(head, newStrTemp).reverse.replaceFirst(tail, newStrTemp.reverse).reverse
    //      else if (regexArray.contains(headStr) && !regexArray.contains(lastStr))
    //        strTemp.replaceFirst(head, newStrTemp)
    //      else if (regexArray.contains(lastStr) && !regexArray.contains(headStr))
    //        strTemp.reverse.replaceFirst(tail, newStrTemp.reverse).reverse
    //      else strTemp
    //      result.trim
    //    }
    //
    //    val uFunc = NullableFunctions.udf(func)
    //
    //    println(func(""))
    //
    //    import org.apache.spark.mllib.clustering.KMeans


    //  val ifNewSchema = inputDf.schema
    //
    //  val colNames = inputDf.schema.filter(field => field.dataType == StringType).map(_.name)
    //
    //  inputDf.selectExpr(colNames:_*).collect().map(row => {
    //    colNames.map(name => {
    //      val value = row.getAs[String](name)
    //      try{
    //        func(value)
    //      }catch{
    //        case e: Exception => throw new Exception(s"在处理${value}时出错，具体信息${e.getMessage}")
    //      }
    //    })
    //  }
    //  ).foreach(arr => println(arr.mkString(",")))
    //
    //  for(each <- ifNewSchema){
    //    val name = each.name
    //    inputDf = inputDf.withColumn(name, uFunc(col(name)))
    //  }
    //
    //  val exprSQL = for (sField <- schema) yield{
    //
    //    if (sField.dataType == StringType) {
    //      val colName = sField.name
    //      println(i)
    //      val strTemp = try{
    //        if (i == 1) {
    //          val head = colName.getBytes()
    //          if (head(0) == (-17) && head(1) == (-69) && head(2) == (-65)) head.drop(3).map(_.toChar).mkString else colName
    //        } else colName
    //      }catch{
    //        case e: Exception => throw new Exception(s"在$colName 处失败，此时i = $i, 错误${e.getMessage}")
    //      }
    //      //        val newName=colName.substring(colName.indexOf("\"")+1,colName.lastIndexOf("\""))
    //      //        val newName = colName.trim.slice(1, colName.length - 1)
    //      val newName = try{
    //        func(strTemp.trim)
    //      }catch {
    //        case e: Exception => throw new Exception(s"在此处失败，错误在${strTemp.trim},错误${e.getMessage}")
    //      }
    //
    //      (colName, newName)
    //    }else{
    //      ("f", "f")
    //    }
    //  }
    //
    //  inputDf = inputDf.selectExpr(exprSQL.map{case (colN, newN) => "`" + colN + "`" + " as " + "`" + newN + "`"}:_*)
    //
    //  inputDf.show
    //
    //
    //
    //  val outDf = inputDf
    //  outDf.cache()
    //  outputrdd.put("<#rddtablename#>", outDf)
    //  outDf.registerTempTable("<#rddtablename#>")
    //  sqlc.cacheTable("<#rddtablename#>")


  }


}
