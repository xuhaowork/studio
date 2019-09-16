package com.self.core.constuctGraphByEdge

import com.self.core.baseApp.myAPP
import com.self.core.constuctGraphByEdge.models.{EdgeProperty, VertexProperty}

object constructGraphByEdge extends myAPP {
  def createData(): Unit = {
    import java.sql.Timestamp
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._

    val rdd = sc.textFile("G://网络行为分析数据/testdata/spam.txt")
    val firstLine = rdd.first()
    val schema = rdd.first().split(",").map(name => StructField(name.replaceAll("\"", ""), DoubleType)) :+ StructField("time", TimestampType)

    val rowRdd = rdd.filter(s => s != firstLine).map {
      s =>
        val arr = s.split(",")
        val spam = if (arr(5) == "spam") 1.0 else 0.0
        Row.merge(
          Row(scala.math.floor(arr(0).toDouble * 30)),
          Row(scala.math.floor(arr(4).toDouble * 30)),
          Row.fromSeq(arr.slice(1, 4).map(value => scala.math.floor(value.toDouble * 1000)) :+ spam),
          Row(new Timestamp(0L))
        )
    }

    val newDF = sqlc.createDataFrame(rowRdd, StructType(schema))
    newDF.show()

    val rddTableName = "testData"

    outputrdd.put(rddTableName, newDF)
  }


  override def run(): Unit = {
    import com.google.gson.JsonParser
    import com.self.core.featurePretreatment.utils.{Tools => featurePtTools}
    import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
    import org.apache.spark.rdd.RDD
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.{DataFrame, Row}

    /** 0)系统变量 */
    val jsonparam = "<#jsonparam#>"
    val z1 = outputrdd
    val rddTableName = "<#zzjzRddName#>"

    /** 1)参数解析 */
    val parser = new JsonParser()
    val jsonParamObj = parser.parse(jsonparam).getAsJsonObject

    val inputTableName = jsonParamObj.get("inputTableName").getAsString
    val srcVertexCol = jsonParamObj.get("srcVertexCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
    val dstVertexCol = jsonParamObj.get("dstVertexCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
    val distinct = jsonParamObj.get("distinct").getAsString == "true"
    val vertexFrequencyType = jsonParamObj.get("vertexFrequencyType").getAsString // one, connectFrequency, pageRank

    val srcVertexTag = jsonParamObj.get("srcVertexTag").getAsString.toInt // 单选框，必选，不需要try判定，下同
    val dstVertexTag = jsonParamObj.get("dstVertexTag").getAsString.toInt
    val edgeTag = jsonParamObj.get("edgeTag").getAsString.toInt // 结束下同

    /** 2)输入数据的必要条件判定 */
    val rawDataFrame = z1.get(inputTableName).asInstanceOf[DataFrame]
    try {
      rawDataFrame.schema.length
    } catch {
      case _: Exception => throw new Exception(s"您输入的表'$inputTableName'没有找到，" +
        s"请检查是否与前序结点输出一致或前序结点是否执行.")
    }

    featurePtTools.columnExists(srcVertexCol, rawDataFrame, true)
    featurePtTools.columnExists(dstVertexCol, rawDataFrame, true)
    import org.apache.spark.sql.columnUtils.DataTypeImpl._
    val srcDT = rawDataFrame.schema(srcVertexCol).dataType
    require(
      srcDT in "numeric",
      s"您输入的列'$srcVertexCol'类型'${srcDT.simpleString}'不是long、int、double等数值类型"
    )
    val dstDT = rawDataFrame.schema(dstVertexCol).dataType
    require(
      dstDT in "numeric",
      s"您输入的列'$dstVertexCol'类型'${dstDT.simpleString}'不是long、int、double等数值类型"
    )

    /** 3)边数据 */
    val edgeRDD: RDD[Edge[EdgeProperty]] = rawDataFrame.select(
      col(srcVertexCol), col(dstVertexCol)
    ).rdd.map {
      row =>
        util.Try {
          (true, Edge(row(0).toString.toDouble.toLong,
            row(1).toString.toDouble.toLong,
            new EdgeProperty().setTag(edgeTag)
          ))
        } getOrElse(false, Edge(0L, 0L, new EdgeProperty().setTag(0L)))
    }.filter(_._1).values


    /** 3)构建图 */
    val vertexRdd: RDD[(VertexId, VertexProperty)] = edgeRDD.map(e => ((e.srcId, e.dstId), 0))
      .aggregateByKey(0)((_, _) => 0, (res1, _) => res1).keys.flatMap {
      case (srcId, dstId) =>
        Array(
          (srcId, new VertexProperty().setVertexType(srcVertexTag)),
          (dstId, new VertexProperty().setVertexType(dstVertexTag))
        )
    }

    val graph: Graph[VertexProperty, EdgeProperty] = if (distinct) {
      Graph(vertexRdd, edgeRDD)
        .groupEdges((edgeProperty1, edgeProperty2) => edgeProperty1.add(edgeProperty2))
    } else {
      Graph(vertexRdd, edgeRDD)
    }

    /** 4)将边数据和顶点数据装进DataFrame用于图元展示 */
    val rowRdd4Vertex: RDD[Row] = vertexFrequencyType match {
      case "one" =>
        graph.vertices.map {
          case (id, property) =>
            Row.fromTuple(
              (id, property.getVertexType.toString, 1.0)
            )
        }

      case "connectFrequency" =>
        graph.aggregateMessages[(VertexProperty, Int)](
          edgeMessage => {
            edgeMessage.sendToSrc((edgeMessage.srcAttr, 1))
            edgeMessage.sendToDst((edgeMessage.dstAttr, 1))
          }, (a1, a2) => (a1._1, a1._2 + a2._2)
        )
          .map {
            case (id, (property, frequency)) =>
              Row.fromTuple(
                (id, property.getVertexType.toString, frequency.toDouble)
              )
          }

      case "pageRank" =>
        val vertex1: VertexRDD[VertexProperty] = graph.vertices
        val vertex2: VertexRDD[Double] = graph.pageRank(0.001).vertices

        vertex1.join(vertex2).map {
          case (id, (property, frequency)) =>
            Row.fromTuple(id, property.getVertexType.toString, frequency)
        }
    }

    val vertexDF = rawDataFrame.sqlContext.createDataFrame(
      rowRdd4Vertex,
      StructType(
        Array(
          StructField("vertexId", LongType), StructField("vertexType", StringType),
          StructField("frequency", DoubleType)
        )
      )
    )

    val rowRdd4Edge: RDD[Row] = graph.edges.map {
      edge =>
        Row(edge.srcId, edge.dstId, edge.attr.getEdgeType, edge.attr.getFrequency)
    }

    val edgeDF = rawDataFrame.sqlContext.createDataFrame(
      rowRdd4Edge,
      StructType(
        Array(
          StructField("srcId", LongType), StructField("dstId", LongType), StructField("edgeType", IntegerType),
          StructField("frequency", LongType)
        )
      )
    )

    /** 5)输出数据：顶点数据、边数据和图 */
    outputrdd.put(rddTableName + "_vertexDF", vertexDF)
    sqlc.cacheTable(rddTableName + "_vertexDF")

    outputrdd.put(rddTableName + "_edgeDF", edgeDF)
    sqlc.cacheTable(rddTableName + "_edgeDF")

  }
}
