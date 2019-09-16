package org.apache.spark.sql.dbPartiiton.jdbc

import java.sql.{Connection, ResultSet}

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.dbPartiiton.service.ResConx
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class StringPartition(idx: Int, val lower:String, val upper:String) extends Partition {
  override def index: Int = idx
}

class StringRDD[T: ClassTag](
                                  sc: SparkContext,
                                  getConnection: () => Connection,
                                  sql: String,  //sql+"where "+Field+"<"+field+" and "+Field+">="+field
                                  field:String,
                                  partion:Array[String],
                                  fetchSize:Int,
                                  mapRow: (ResultSet) => T = StringRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {



  override def getPartitions: Array[Partition] = {

    (0 until partion.length-1).map(i => {
      val start=partion(i)
      val end =partion(i+1)
      new StringPartition(i, start, end)
    }).toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = new NextIterator[T] {
    context.addTaskCompletionListener { context => closeIfNeeded() }
    val conn2 = getConnection()
    val stmt2 = conn2.createStatement()
    val part = thePart.asInstanceOf[StringPartition]
    val url = conn2.getMetaData.getURL

    if (conn2.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt2.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt2.getFetchSize + " to force MySQL streaming ")
    }else  {
      stmt2.setFetchSize(fetchSize)
    }
    var sql2=""
    if (sql.contains("where")){    // don't  solve  Uppercase  problem
      val temp=sql.split("where")
      if((partion.length-2)==part.index)
        sql2=temp(0)+" where "+field+">="+part.lower+" and "+field+"<="+part.upper+" and "+temp(1)
      else
        sql2=temp(0)+" where "+field+">="+part.lower+" and "+field+"<"+part.upper+" and "+temp(1)
    }else  {
      if((partion.length-2)==part.index)
        sql2=sql+" where "+field+">="+part.lower+" and "+field+"<="+part.upper
      else
        sql2=sql+" where "+field+">="+part.lower+" and "+field+"<"+part.upper
    }
     println(sql2)
    val rs2 = stmt2.executeQuery(sql2)
    type Closeable = {
      def close:Unit
    }

    /**
      * 调用此段代码 要么写好连接池的释放，要么关闭连接池
    * */

  /*  ResConx.getInstance().conx().retRes(conn2.asInstanceOf[Closeable])
    new org.apache.log4j.helpers.SyslogQuietWriter(new org.apache.log4j.helpers.SyslogWriter("192.168.15.25:514"), 600,
      new org.apache.log4j.varia.FallbackErrorHandler()).write("after ret pool size:" + ResConx.getInstance().conx() + ":" +  ResConx.getInstance().conx().count())
*/

    override def close(): Unit = {
      stmt2.close
      // 以url作为连接池的key
      ResConx.getInstance().conx(url).retRes(conn2.asInstanceOf[Closeable])
    }

    override def getNext(): T = {
      if (rs2.next()) {
        mapRow(rs2)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }
  }
}


object StringRDD {
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
                 field:String,
                 partition: Array[String],
                 fetchSize:Int,
                 mapRow: JFunction[ResultSet, T]): JavaRDD[T] = {

    val StringRDD = new StringRDD[T](
      sc.sc,
      () => connectionFactory.getConnection,
      sql,
      field,
      partition,
      fetchSize,
      (resultSet: ResultSet) => mapRow.call(resultSet))(fakeClassTag)
    new JavaRDD[T](StringRDD)(fakeClassTag)
  }

  def create(
              sc: JavaSparkContext,
              connectionFactory: ConnectionFactory,
              sql: String,
              field:String,
              partition:Array[String],
              fetchSize:Int
            ): JavaRDD[Array[Object]] = {

    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }
    create(sc, connectionFactory,sql,field,partition,fetchSize,mapRow)
  }
}
