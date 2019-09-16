package org.apache.spark.sql.dbPartiiton.jdbc

import java.sql.{Connection, ResultSet}

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.dbPartiiton.service.ResConx
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag


class RowNumPartition(idx: Int, val lower:Long, val upper:Long) extends Partition {
  override def index: Int = idx
}
class RowNumRDD[T: ClassTag](
                            sc: SparkContext,
                            getConnection: ( ) =>Connection,
                            DBType:String,
                            sql: String,
                            lowerBound:Long,
                            upperBound:Long,
                            partiNum: Int,
                            fetchSize:Int,
                            mapRow: (ResultSet) => T = RowNumRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {
   override def getPartitions: Array[Partition] = {
     val bounds=new Array[Long](partiNum+1)
      bounds(0)=lowerBound
      bounds(partiNum)=upperBound

     if(partiNum>1) {
       for (i <- 1 to partiNum - 1) {
         val index =lowerBound+(upperBound-lowerBound+1)*i/partiNum
         bounds(i)=index
       }
     }
    (0 until partiNum).map(i => {
      val start =bounds(i)
      val end = bounds(i+1)
      new RowNumPartition(i, start, end)
    }).toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = new NextIterator[T] {
      context.addTaskCompletionListener { context => closeIfNeeded() }
      val conn2 = getConnection()
      val stmt2 = conn2.createStatement()
      val part = thePart.asInstanceOf[RowNumPartition]
      val url = conn2.getMetaData.getURL

      var sql2=""
      var rs2:ResultSet=null

      if (conn2.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt2.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt2.getFetchSize + " to force MySQL streaming ")
     } else{
        stmt2.setFetchSize(fetchSize)
      }

    if (DBType=="Oracle"){
       sql2 = "SELECT * FROM ( SELECT A.*,ROWNUM num FROM ( " + sql + " ) A  WHERE  ROWNUM<" + part.upper + " )  WHERE num>=" + part.lower

       rs2 = stmt2.executeQuery(sql2)
    } else if(DBType=="MySQL" || DBType=="Gbase8a" ) {
      sql2 = sql + " limit " + part.lower + "," + (part.upper - part.lower)
      if (sql.contains("limit")) {
        val step=part.upper-part.lower
        val start=part.lower
        sql2 = s"select * from ($sql) a limit $start,$step"
      }

      rs2 = stmt2.executeQuery(sql2)
    } else if(DBType=="Gbase8t"){
      val  tem="select skip "+part.lower+" first "+(part.upper-part.lower)
      sql2=sql.replaceAll("[Ss][Ee][Ll][Ee][Cc][Tt]",tem)

      rs2 = stmt2.executeQuery(sql2)
    } else if(DBType=="Postgre" || DBType=="MPPDB" ){

      sql2=sql+" limit "+(part.upper-part.lower)+" offset "+part.lower
      if (sql.contains("limit")) {
        val step=part.upper-part.lower
        val start=part.lower
        sql2 = s"select * from ($sql) a limit $step offset $start"
      }

      rs2 = stmt2.executeQuery(sql2)

    }  else{
      println("此数据库不支持默认分区")
    }

    println(sql2)

    type Closeable = {
      def close:Unit
    }
//    ResConx.getInstance().conx(url).retRes(conn2.asInstanceOf[Closeable])
//
//    new org.apache.log4j.helpers.SyslogQuietWriter(new org.apache.log4j.helpers.SyslogWriter("192.168.15.25:514"), 600,
//      new org.apache.log4j.varia.FallbackErrorHandler()).write("after ret pool size:" + ResConx.getInstance().conx(url) + ":" +  ResConx.getInstance().conx(url).count())


    override def getNext(): T = {
        if (rs2.next()) {
          mapRow(rs2)
        } else {
          finished = true
          null.asInstanceOf[T]
        }
      }

      override def close() {
        stmt2.close
        // 以url作为连接池的key
        ResConx.getInstance().conx(url).retRes(conn2.asInstanceOf[Closeable])
      }
    }
  }

object RowNumRDD {
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
                 DBType:String,
                 sql: String,
                 lowerBound:Long,
                 upperBound:Long,
                 partiNum:Int,
                 fetchSize:Int,
                 mapRow: JFunction[ResultSet, T]): JavaRDD[T] = {

    val RowNumRDD = new RowNumRDD[T](
      sc.sc,
      () => connectionFactory.getConnection,
      DBType,
      sql,
      lowerBound,
      upperBound,
      partiNum,
      fetchSize,
      (resultSet: ResultSet) => mapRow.call(resultSet))(fakeClassTag)
    new JavaRDD[T](RowNumRDD)(fakeClassTag)
  }
  def create(
              sc: JavaSparkContext,
              connectionFactory: ConnectionFactory,
              DBType:String,
              sql: String,
              lowerBound:Long,
              upperBound:Long,
              partiNum:Int,
              fetchSize:Int
            ): JavaRDD[Array[Object]] = {

    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }
    create(sc, connectionFactory,DBType,sql,lowerBound,upperBound,partiNum,fetchSize,mapRow)
  }
}
