package org.apache.spark.sql.dbPartiiton.jdbc

import java.sql.{Connection, ResultSet}

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.dbPartiiton.service.ResConx
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class LongPartition(idx: Int, val lower:Long, val upper:Long) extends Partition {
  override def index: Int = idx
}

class LongRDD[T: ClassTag](
                           sc: SparkContext,
                           getConnection: () => Connection,
                           sql: String,    // sql+"where "+Field+"<"+?+" and "+Field+">="+?
                           partition:Array[Long],
                           fetchSize:Int,
                           mapRow: (ResultSet) => T = LongRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    (0 until partition.length-1).map(i => {
      val start =partition(i)
      val end = partition(i+1)
      new LongPartition(i, start, end)
    }).toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = new NextIterator[T] {
    context.addTaskCompletionListener { context => closeIfNeeded() }
    val conn2 = getConnection()
    val stmt2 = conn2.prepareStatement(sql)
    val part = thePart.asInstanceOf[LongPartition]
    val url = conn2.getMetaData.getURL

    if (conn2.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt2.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt2.getFetchSize + " to force MySQL streaming ")
    }
    else {stmt2.setFetchSize(fetchSize)
    }

    stmt2.setLong(1, part.lower)
    stmt2.setLong(2, part.upper)

    val rs2 = stmt2.executeQuery()

  /**
  *   调用的时候，一定做好连接池的释放或者连接的关闭。
  * */


    type Closeable = {
      def close:Unit;
    }

//    ResConx.getInstance().conx().retRes(conn2.asInstanceOf[Closeable])

    override def getNext(): T = {
      if (rs2.next()) {
        mapRow(rs2)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close(){
      stmt2.close
      // 以url作为连接池的key
      ResConx.getInstance().conx(url).retRes(conn2.asInstanceOf[Closeable])

    }

  }
}

object LongRDD {
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }
  trait ConnectionFactory extends Serializable {
    @throws[Exception]
    def getConnection: Connection
  }
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
  def create[T](
                 sc: JavaSparkContext,
                 connectionFactory: ConnectionFactory,
                 sql: String,
                 partition:Array[Long],
                 fetchSize:Int,
                 mapRow: JFunction[ResultSet, T]): JavaRDD[T] = {

    val LongRDD = new LongRDD[T](
      sc.sc,
      () => connectionFactory.getConnection,
      sql,
      partition,
      fetchSize,
      (resultSet: ResultSet) => mapRow.call(resultSet))(fakeClassTag)
    new JavaRDD[T](LongRDD)(fakeClassTag)
  }

  def create(
              sc: JavaSparkContext,
              connectionFactory: ConnectionFactory,
              sql: String,
              partition: Array[Long],
              fetchSize:Int
            ): JavaRDD[Array[Object]] = {

    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }
    create(sc,connectionFactory,sql,partition,fetchSize,mapRow)
  }
}
