package org.apache.spark.sql.dbPartiiton.jdbc

import java.sql.{Connection, ResultSet}

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.dbPartiiton.service.ResConx
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag


class JdbcPartition(idx: Int, val Course:String) extends Partition {
  override def index: Int = idx
}

class JdbcRDD[T: ClassTag](
                              sc: SparkContext,
                              getConnection: () => Connection,
                              sql: String,
                              partion:Array[String],
                              fetchSize:Int,
                              mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {


  override def getPartitions: Array[Partition] = {
    if (partion == null){
      val arr = new Array[Partition](1)
      arr(0) = new JdbcPartition(0, "")
      arr
    }
    else {
      (0 to partion.length - 1).map(i => {
        val tem = partion(i)
        new JdbcPartition(i, tem)
      }).toArray
    }
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = new NextIterator[T] {
    context.addTaskCompletionListener { context => closeIfNeeded() }
    val conn2 = getConnection()
    val stmt2 = conn2.createStatement()
    val part = thePart.asInstanceOf[JdbcPartition]
    val url = conn2.getMetaData.getURL

    if (conn2.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt2.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt2.getFetchSize + " to force MySQL streaming ")
    } else {
      stmt2.setFetchSize(fetchSize)
    }
    var sql2 = ""
    if (sql.contains("where")) {
      // don't  solve  Uppercase  problem
      val temp = sql.split("where")
      sql2 = temp(0) + " where " + part.Course + " and " + temp(1)
    }
    else if(sql.contains("limit")) {
      if(part.Course.length>0) {
        val whe=part.Course
        sql2 = s"select * from ($sql) a where $whe"
      }
      else
        {
          sql2=sql
        }
    }
    else {
      if (part.Course.length > 0) {
        sql2 = sql + " where " + part.Course
      } else {
        sql2 = sql
      }
    }

    val rs2 = stmt2.executeQuery(sql2)
    println(sql2)

    type Closeable = {
      def close: Unit;
    }
    // 以url作为连接池的key
    //    ResConx.getInstance().conx(url).retRes(conn2.asInstanceOf[Closeable])

    /*
    new org.apache.log4j.helpers.SyslogQuietWriter(new org.apache.log4j.helpers.SyslogWriter("192.168.15.25:514"), 600,
      new org.apache.log4j.varia.FallbackErrorHandler()).write("after ret pool size:" + ResConx.getInstance().conx(url) + ":" +  ResConx.getInstance().conx(url).count())
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

object JdbcRDD {
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
                 partition: Array[String],
                 fetchSize:Int,
                 mapRow: JFunction[ResultSet, T]): JavaRDD[T] = {

    val JdbcRDD = new JdbcRDD[T](
      sc.sc,
      () => connectionFactory.getConnection,
      sql,
      partition,
      fetchSize,
      (resultSet: ResultSet) => mapRow.call(resultSet))(fakeClassTag)
    new JavaRDD[T](JdbcRDD)(fakeClassTag)
  }

  def create(
              sc: JavaSparkContext,
              connectionFactory: ConnectionFactory,
              sql: String,
              partition:Array[String],
              fetchSize:Int
            ): JavaRDD[Array[Object]] = {

    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }
    create(sc, connectionFactory,sql,partition,fetchSize,mapRow)
  }
}
