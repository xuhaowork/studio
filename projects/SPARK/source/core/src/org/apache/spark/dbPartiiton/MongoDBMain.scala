package org.apache.spark.sql.dbPartiiton

import com.google.gson.{JsonArray, JsonObject, JsonParser}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD
import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.utils.ToolUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.dbPartiiton.partition.Analysis
import org.bson
import org.bson.{BsonDocument, Document}
import org.apache.spark.sql.types._

import scala.util.parsing.json.JSONArray

/**
  * MongoDB并行加载
  * author: Bai yuan
  * date:   2018/1/19
  **/

object MongoDBMain extends BaseMain {

  override def run: Unit = {

    /*
    val jsonparam =
      """{"DBType":{"value":"mongodb"},
        |"PartitionType":{"value":"MongoDefaultPartitioner","PartitionKey":"_id","PartitionSizeMB":"64 MB","SamplesPerPartition":"10"},
        |"host":"192.168.11.110",
        |"port":"10005","ConnType":"Host","Base":"json","Table":"METADATA_SatDowDemDec",
        |"Field":[{"displayName":"CNR(Double)","name":"CNR","datatype":"Double"},{"displayName":"CRCCodGenPol(String)","name":"CRCCodGenPol","datatype":"String"},{"displayName":"EBEMFECParII(String)","name":"EBEMFECParII","datatype":"String"}],
        |"Filter":{"displayName":"ObjID>1,captureTime>5,carTraInsNum<6,ts>"20180122"","allData":["ObjID(String)","antennaDirection(String)","captureTime(String)","carTraInsNum(Integer)","ts(Date)"]}}""".stripMargin
*/

    /*       val jsonparam =
      """{"Base":"test",
        |"ConnType":"Host",
        |"DBType":{"value":"mongodb"},
        |"Field":[{"datatype":"Double","displayName":"CNR(Double)","name":"CNR"},{"datatype":"Integer","displayName":"EBEMFECParI(Integer)","name":"EBEMFECParI"}],
        |"Filter":{"allData":["MSStaTim(String)"],"displayName":""},
        |"PartitionType":{"PartitionKey":[{"datatype":"Integer","displayName":"EBEMFECParI(Integer)","name":"EBEMFECParI"}],"PartitionSizeMB":"64 MB","SamplesPerPartition":"10","value":"MongoDefaultPartitioner"},
        |"RERUNNING":{"nodeName":"MongoDB并行加载_1","preNodes":[],"rerun":"false"},
        |"Table":"METADATA_TesDemDec",
        |"host":"192.168.11.110",
        |"port":"10005"}""".stripMargin*/

    /*       val jsonparam =
      """{"Base":"admin",
        |"ConnType":"Host",
        |"DBType":{"value":"mongodb"},
        |"Field":[{"datatype":"String","displayName":"name(String)","name":"name"},{"datatype":"Double","displayName":"age(Double)","name":"age"}],
        |"Filter":{"allData":["age(Double)"],"displayName":"age=10"},
        |"PartitionType":{"PartitionKey":[{"datatype":"ObjectId","displayName":"_id(ObjectId)","name":"_id"}],"PartitionSizeMB":"64 MB","SamplesPerPartition":"10","value":"MongoDefaultPartitioner"},
        |"RERUNNING":{"nodeName":"MongoDB并行加载_1复件1","preNodes":[],"rerun":"false"},
        |"Table":"testDemo1",
        |"host":"192.168.15.15",
        |"passw":"admin",
        |"port":"27017",
        |"user":"admin"}""".stripMargin*/

    /*       val jsonparam =
      """{"Base":"admin",
        |"ConnType":"Host",
        |"DBType":{"value":"mongodb"},
        |"Field":[{"datatype":"String","displayName":"name(String)","name":"name"},{"datatype":"Double","displayName":"age(Double)","name":"age"}],
        |"Filter":{"allData":["age(Double)"],"displayName":"age=10,name='USER10'"},
        |"PartitionType":{"PartitionKey":[{"datatype":"ObjectId","displayName":"_id(ObjectId)","name":"_id"}],"PartitionSizeMB":"64 MB","SamplesPerPartition":"10","value":"MongoDefaultPartitioner"},
        |"RERUNNING":{"nodeName":"MongoDB并行加载_1复件2","preNodes":[],"rerun":"false"},
        |"Table":"testDemo1",
        |"host":"192.168.15.15",
        |"passw":"admin",
        |"port":"27017",
        |"user":"admin"}""".stripMargin*/

    //       val jsonparam =
    //         """{"Base":"test",
    //           |"ConnType":"Host",
    //           |"DBType":{"value":"mongodb"},
    //           |"Field":[{"datatype":"Double","displayName":"CNR(Double)","name":"CNR"},{"datatype":"Integer","displayName":"EBEMFECParI(Integer)","name":"EBEMFECParI"},{"datatype":"String","displayName":"EBEMFECParII(String)","name":"EBEMFECParII"},{"datatype":"Boolean","displayName":"phaAmb(Boolean)","name":"phaAmb"}],
    //           |"Filter":{"allData":["age(Double)","CNR(Double)"],"displayName":"CNR>20"},
    //           |"PartitionType":{"PartitionKey":[{"datatype":"ObjectId","displayName":"_id(ObjectId)","name":"_id"}],"PartitionSizeMB":"64 MB","SamplesPerPartition":"10","value":"MongoDefaultPartitioner"},
    //           |"RERUNNING":{"nodeName":"MongoDB并行加载_1复件2","preNodes":[],"rerun":"false"},
    //           |"Table":"METADATA_TesDemDec",
    //           |"host":"192.168.11.110",
    //           |"passw":"admin",
    //           |"port":"10005",
    //           |"user":"admin"}""".stripMargin

    /*val jsonparam =
"""{"Base":"test",
 |"ConnType":"Host",
 |"DBType":{"value":"mongodb"},
 |"Field":[{"datatype":"Double","displayName":"age(Double)","name":"age"}],
 |"PartitionType":{"PartitionKey":[{"datatype":"ObjectId","displayName":"_id(ObjectId)","name":"_id"}],
 |"PartitionSizeMB":"64 MB","SamplesPerPartition":"10","value":"MongoDefaultPartitioner"},
 |"RERUNNING":{"nodeName":"MongoDB并行加载_2","preNodes":[],"rerun":"false"},
 |"Table":"test",
 |"host":"192.168.11.34",
 |"passw":"admin",
 |"port":"27017",
 |"user":"admin"}""".stripMargin*/

    val jsonparam =
      """{"Base":"test",
        |"ConnType":"Host",
        |"DBType":{"value":"mongodb"},
        |"Field":[{"datatype":"Integer","displayName":"id(Integer)","name":"id"},{"datatype":"Integer","displayName":"age(Integer)","name":"age"}],
        |"Filter":"[object Object]",
        |"PartitionType":{"PartitionKey":[{"datatype":"ObjectId","displayName":"_id(ObjectId)","name":"_id"}],
        |"PartitionSizeMB":"64 MB","SamplesPerPartition":"10","value":"MongoDefaultPartitioner"},
        |"RERUNNING":{"nodeName":"MongoDB并行加载_2复件1","preNodes":[],"rerun":"false"},
        |"Table":"bank_t",
        |"host":"192.168.11.26",
        |"port":"27017"}""".stripMargin


    //       val jsonparam = "<#jsonparam#>"

    println(jsonparam)

    def conv2DType(dtype: String): DataType = {
      val dftype = dtype match {
        case "String" => StringType
        case "Double" => DoubleType
        case "Float" => FloatType
        case "Integer" => IntegerType
        case "Timestamp" => TimestampType
        case "Date" => DateType
        case "Long" => LongType
        case "Boolean" => BooleanType
        case "Binary Data" => BinaryType
        case _ => StringType
      }
      dftype
    }

    val parser = new JsonParser()
    val temJsonParser = parser.parse(jsonparam).getAsJsonObject
    val host = temJsonParser.get("host").getAsString
    val port = temJsonParser.get("port").getAsString
    val base = temJsonParser.get("Base").getAsString
    val table = temJsonParser.get("Table").getAsString

    val fieldTem = temJsonParser.getAsJsonArray("Field")
    val colNums = fieldTem.size()
    val schemaSeq = for (i <- 0 until colNums) yield {
      val colJsonObj = fieldTem.get(i).getAsJsonObject
      val name = colJsonObj.get("name").getAsString
      val datatype = colJsonObj.get("datatype").getAsString

      StructField(name, conv2DType(datatype), true)
    }
    val schema = StructType(schemaSeq)

    val partiitionType = temJsonParser.getAsJsonObject("PartitionType")
    val filterTem = try {
      temJsonParser.getAsJsonObject("Filter")
    }
    catch {
      case e: Exception => null
    }
    //filter  cause
    val filter = filterTem match {
      case null => ""
      case _ => filterTem.get("displayName").getAsString
    }

    val dataType = filterTem match {
      case null => null
      case _ => filterTem.getAsJsonArray("allData")
    }
    //  define function  field  match  data type
    val column = Analysis.mongoSelect(fieldTem)
    val mongodbUrl = "mongodb://" + host + ":" + port + "/" + base + "." + table
    val rdd = partiitionType.get("value").getAsString match {
      case "MongoDefaultPartitioner" =>
        val partitionKey = partiitionType.getAsJsonArray("PartitionKey").get(0).getAsJsonObject.get("name").getAsString
        val partitionSizeMB = partiitionType.get("PartitionSizeMB").getAsString.toUpperCase.replaceAll(" ", "").replaceAll("MB", "")
        // 数据验证
        ToolUtils.toRangeType[Long](partitionSizeMB, 0, Long.MaxValue, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Long.MaxValue}]")
        val samplesPerPartition = partiitionType.get("SamplesPerPartition").getAsString
        // 数据验证
        ToolUtils.toRangeType[Int](samplesPerPartition, 0, Integer.MAX_VALUE, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Integer.MAX_VALUE}]")
        MongoSpark.load[BsonDocument](sc, ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "DefaultMongoPartitioner",
          "partitionKey" -> partitionKey, "partitionSizeMB" -> partitionSizeMB, "samplesPerPartition" -> samplesPerPartition)))

      case "MongoShardedPartitioner" =>
        val partitionKey = partiitionType.getAsJsonArray("Shardkey").get(0).getAsJsonObject.get("name").getAsString
        MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "spark.mongodb.input.partitioner" -> "MongoShardedPartitioner",
          "shardKey" -> partitionKey)))
      case "MongoSplitVectorPartitioner" =>
        val partitionKey = partiitionType.getAsJsonArray("PartitionKey").get(0).getAsJsonObject.get("name").getAsString
        val partitionSizeMB = partiitionType.get("PartitionSizeMB").getAsString.toUpperCase.replaceAll(" ", "").replaceAll("MB", "")
        // 数据验证
        ToolUtils.toRangeType[Long](partitionSizeMB, 0, Long.MaxValue, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Long.MaxValue}]")
        MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "MongoSplitVectorPartitioner",
          "partitionKey" -> partitionKey, "partitionSizeMB" -> partitionSizeMB)))
      case "MongoPaginateByCountPartitioner" =>
        val partitionKey = partiitionType.getAsJsonArray("PartitionKey").get(0).getAsJsonObject.get("name").getAsString
        val numberOfPartitions = partiitionType.get("NumberOfPartitions").getAsString.toUpperCase.replaceAll(" ", "")
        //数据验证
        ToolUtils.toRangeType[Int](numberOfPartitions, 0, Integer.MAX_VALUE, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Integer.MAX_VALUE}]")
        MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "MongoPaginateByCountPartitioner",
          "partitionKey" -> partitionKey, "numberOfPartitions" -> numberOfPartitions)))
      case "MongoPaginateBySizePartitioner" =>
        val partitionKey = partiitionType.getAsJsonArray("PartitionKey").get(0).getAsJsonObject.get("name").getAsString
        val partitionSizeMB = partiitionType.get("PartitionSizeMB").getAsString.toUpperCase.replaceAll(" ", "").replaceAll("MB", "")
        // 数据验证
        ToolUtils.toRangeType[Long](partitionSizeMB, 0, Long.MaxValue, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Long.MaxValue}]")
        MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
          "partitionKey" -> partitionKey, "partitionSizeMB" -> partitionSizeMB)))
    }

    var outDf: DataFrame = null
    // 构造Projection
    val projection = new BsonDocument("$project", BsonDocument.parse(column))
    if (filter.length == 0) {
      val aggregatedRDD = rdd.withPipeline(Seq(projection))
      if (aggregatedRDD.isEmpty())
        outDf = aggregatedRDD.toDF(schema)
      else
        outDf = aggregatedRDD.toDF()
      outDf.show()
      println("end")
    }
    else {
      val query = Analysis.monsqlCov(filter, dataType)
      val queryMongo = new bson.Document("$match", BsonDocument.parse(query))
      //val matchQuery = new Document("$match", BsonDocument.parse("{\"type\":\"1\"}"))
      val aggregatedRDD = rdd.withPipeline(Seq(queryMongo, projection))
      //  val aggregatedRDD = rdd.withPipeline(Seq(projection1))

      if (aggregatedRDD.isEmpty())
        outDf = aggregatedRDD.toDF(schema)
      else
        outDf = aggregatedRDD.toDF()
      outDf.show()
      println("end")
    }
    outputrdd.put("<#rddtablename#>", outDf)
    outDf.registerTempTable("<#rddtablename#>")
    sqlc.cacheTable("<#rddtablename#>")

  }
}
