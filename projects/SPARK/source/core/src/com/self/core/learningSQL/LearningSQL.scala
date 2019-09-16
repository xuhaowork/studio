package com.self.core.learningSQL

import com.self.core.baseApp.myAPP
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.ml.feature._


/**
  * Created by DataShoe on 2018/1/30.
  */
object LearningSQL extends myAPP {
  /** 自定义一个DataType类型 */
  def testDefineType() = {
    case class person(name: String, age: Int)
    class PersonType extends DataType {
      override def defaultSize: Int = 0

      override def asNullable: PersonType = this
    }

    case object PersonType extends PersonType
    val rdd = sc.parallelize(Seq(
      Row(0, 2, person("Lina", 12)),
      Row(1, 2, person("John", 51)),
      Row(2, 2, person("Bill", 61))
    ))


    val df: DataFrame = sqlc.createDataFrame(rdd, StructType(Array(StructField("id", IntegerType), StructField("age", IntegerType), StructField("person", PersonType))))
    df.show()
    df
  }

  def createDF() = {
    val schema2 = StructType(
      Array(
        StructField("id", IntegerType),
        StructField("point", DoubleType)
      ))
    val rdd2 = sc.parallelize(Seq(
      Row(0, 0.1), // Row.apply生成一个GenericRow => new GenericRow(values.toArray)
      Row(1, 0.2),
      Row(2, 0.3)
    ))
    val df2: DataFrame = sqlc.createDataFrame(rdd2, // 将RDD[GenericRow]转为RDD[InternalRow]
      schema2) // 再讲RDD[GenericRow]加上一些表头信息变为LogicalRDD(output: Seq[Attribute], rdd: RDD[InternalRow])
    df2.show()
    df2
  }

  def transformDF(df1: DataFrame, df2: DataFrame) = {
    val newDF = df1.join(df2, Seq("id"), "inner").select("id", "age")
    newDF.show()
    newDF
  }

  def actionDF(df: DataFrame) = {
    df.collect()
  }

  def testCache(cacheType: String, newTableAction: Boolean) = {
    val dataRdd = sc.parallelize(Seq(
      ("1", 2, 10),
      ("2", 2, 8),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7),
      ("3", 4, 7)
    )).repartition(11)

    val dataFrame = sqlc.createDataFrame(dataRdd.map(tup => Row.fromTuple(tup)),
      StructType(Array(
        StructField("id", StringType),
        StructField("height", IntegerType),
        StructField("weight", IntegerType)))
    )
    val tableName = "tableName"
    dataFrame.registerTempTable(tableName)
    cacheType match {
      case "cacheTable" =>
        // 1) cacheTable
        sqlc.cacheTable(tableName) // method1 cache by columnar storage
        if (newTableAction) {
          dataFrame.collect()
          val newDataFrame = sqlc.sql("select * from tableName")
          //          newDataFrame.collect() // 此时才触发缓存CacheManager
          // 查看分区数有没有变化
          println(newDataFrame.rdd.partitions.length) // 没有改变分区数 cacheTable列存储只是同一分区内数据的横向作用。
        } else {
          //          dataFrame.collect()
          val newDataFrame = sqlc.sql("select * from tableName")
          newDataFrame.collect() // 此时才触发缓存CacheManager
          // 查看分区数有没有变化
          println(newDataFrame.rdd.partitions.length) // 没有改变分区数 cacheTable列存储只是同一分区内数据的横向作用。
        }
      case "cache" =>
        // 2) cache
        dataFrame.cache()
        if (newTableAction) {
          dataFrame.collect()
          val newDataFrame = sqlc.sql("select * from tableName")
          //          newDataFrame.collect() // 此时才触发缓存CacheManager
          // 查看分区数有没有变化
          println(newDataFrame.rdd.partitions.length) // 没有改变分区数 cacheTable列存储只是同一分区内数据的横向作用。
        } else {
          //          dataFrame.collect()
          val newDataFrame = sqlc.sql("select * from tableName")
          newDataFrame.collect() // 此时才触发缓存CacheManager
          // 查看分区数有没有变化
          println(newDataFrame.rdd.partitions.length) // 没有改变分区数 cacheTable列存储只是同一分区内数据的横向作用。
        }
    }
  }

  override def run(): Unit = {
    /** 第一章  udf篇 */
    /** 1.udf */
    /** 2.定义一个可以实现null值转换的udf */
    // 已经转移到org.apache.spark.sql.NullableFunctions核心思想是通过Option类
    // 注意增加了GC的压力


    /** udaf */
    // 还未进行

    /** 第二章 data types篇 */
    /** 1.自定义一个DataType类型 */
    val df1 = testDefineType().select("id", "age")
    // 可以实现，需要实现defaultSize和asNullable方法
    // 注意增加了GC的压力
    // 不能涉及到写，包括shuffle write，比如join
    // @todo 1)如果将person放到外层join可不可行
    // @todo 2)测试person cache可不可以
    /** 2.meta data */

    /** 3.schema */


    /** 第三章 DataFrame实例化和执行篇 */
    /** 1.探究一下DataFrame和Row等之间的关系 */
    // 有RDD[Row]创建一个DF的过程
    val df2 = createDF() // 将RDD[GenericRow]转为RDD[InternalRow]
    // 再将RDD[InternalRow]加上一些表头信息变为LogicalRDD(output: Seq[Attribute], rdd: RDD[InternalRow])，
    // LogicalRDD继承自LogicalPlan
    // ----
    // 一个DataFrame是由class DataFrame private[sql](override val sqlContext: SQLContext,val queryExecution: QueryExecution)实例化生成的。
    // 其中queryExecution是封装了SQLContext和LogicalPlan的类，其中LogicalPlan起到了决定性作用。
    // 所以创建一个DataFrame过程中生成的LogicalRDD（也就是RDD[InternalRow]和表头信息）起到决定性作用。

    // ----
    // 总体分析
    // ----
    // 结合创建、操作一个DataFrame
    // 除了LogicalRDD外，几乎所有的transformation操作（Projection、aggregate）
    // 都继承自LogicalPlan，而LogicalPlan继承自QueryPlan[LogicalPlan]，是个树结构，可以添加child
    // ----
    // LogicalPlan加上SQLContext变为queryExecution。
    // 归根结底，从创建到操作DataFrame，LogicalPlan最为重要
    // 下面看一下操作DataFrame

    /** 2.探究一下tansform DataFrame */
    val newDF = transformDF(df1, df2)
    newDF.show()
    // 先分析单个transform
    // ----
    // 1）以select为例：
    //    def select(cols: Column*): DataFrame = withPlan {
    //        Project(cols.map(_.named), logicalPlan)
    //        ... ...
    //    }
    // 其中withPlan是由一个DataFrame到另一个DataFrame的创建过程，
    // 即由一个LogicalPlan到另一个LogicalPlan的过程
    // Project是一个LogicalPlan，是UnaryNode（继承了LogicalPlan）的子类，只涉及到一个DF的操作
    // 他具有child节点，project在创建的时候讲之前的LogicalPlan以child节点的形式保存。

    // 2）以join为例：
    // 和前面一样join的LogicalPlan由Join类型实现。
    // 不在赘述，直接看Join类型
    //case class Join(
    //                 left: LogicalPlan,
    //                 right: LogicalPlan, ...) extends BinaryNode {
    // ...
    // }
    // 与select不同，join双节点操作，他有两个子节点。
    // 因此df1.join(df2)最后就会是Join(df1的LogicalPlan, df2的LogicalPlan)
    // BinaryNode继承自LogicalPlan，一些涉及到两表的操作，如union等都和它有关。

    // 在综合分析:
    // ----
    // df1和df2本身创建时的LogicalPlan（前面看到，其实是个LogicalRDD封装后的东东），
    // 1)经过join操作时，新的LogicalPlan变为Join(df1的LogicalPlan, df1的LogicalPlan,...)
    // 2)其次我们又来了一个select，新DataFrame变为Projection(..., Join(...))
    // 3)如此变成一个小的树，每个节点都是一个LogicalPlan，我们还可以继续进行其他操作，
    // 这样树的深度不断延长直至action操作withCallback触发执行

    /** 3.探究一下action操作过程 */
    actionDF(newDF)
    newDF.cache()
    // 以collect操作为例
    // 1）他会在每个Executor中调用newDF的LogicalPlan（加上SQLContext就是queryExecution）的一系列lazy变量：
    // lazy val analyzed: LogicalPlan = sqlContext.analyzer.execute(logical)
    // 解析任务
    // lazy val withCachedData: LogicalPlan = { ...sqlContext.cacheManager.useCachedData(analyzed) }
    // 查看是否需要cache ——注意：这里（version 1.5.2）是新表action触发cache，原表cache再action时并不会触发，下面会讨论
    // lazy val optimizedPlan: LogicalPlan = sqlContext.optimizer.execute(withCachedData)
    // 最优化LogicalPlan的树（上面说过，是树状，这里会根据Rule里面的规则进行优化）
    // lazy val sparkPlan: SparkPlan = { SQLContext.setActive(sqlContext);sqlContext.planner.plan(optimizedPlan).next()}
    // 转换为physicalPlan
    // lazy val executedPlan: SparkPlan = sqlContext.prepareForExecution.execute(sparkPlan)
    // 生成可执行的物理计划

    /** 4.探究一下cache过程 */
    testCache("cache", true) // 触发CacheManager，和cacheTable一致，分区数11
    testCache("cacheTable", true) // 触发CacheManager，和cache一致，分区数11

    testCache("cache", true) // 没有触发CacheManager，和cacheTable一致，分区数11
    testCache("cacheTable", true) // 没有触发CacheManager，和cache一致，分区数11
    // 结论：
    // 1)对DataFrame来说cache和cacheTable一致，都是调用cacheManager.cacheQuery方法
    // 2)一个DataFrame的action不能触发它本身的cache，必须是它下一级的树（以它的LogicalPlan为子节点）的DataFrame才能触发。
    // 3）DataFrame的列式存储没有改变原有的rdd分区结构（不加涉及到重分区的操作），列式存储只是在各个分区的横向变换，
    // 每个分区中： 基于行 => 基于列

  }
}
