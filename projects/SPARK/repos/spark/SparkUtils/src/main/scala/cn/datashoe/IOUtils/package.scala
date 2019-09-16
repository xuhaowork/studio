package cn.datashoe

import java.io.File
import java.nio.charset.Charset
import java.sql.{Connection, ResultSet, ResultSetMetaData, SQLException}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._


package object IOUtils {

  /** mongodb读取 */
  object MongodbIO {

    import cn.datashoe.numericToolkit.Toolkit
    import com.mongodb.spark.MongoSpark
    import com.mongodb.spark.config._
    import com.mongodb.spark.sql._
    import org.apache.spark.SparkContext
    import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.{DataFrame, SparkSession}
    import org.bson
    import org.bson.BsonDocument

    var conf: Map[String, String] = Map("port" -> "27017")

    def setHost(host: String): this.type = {
      conf += "host" -> host
      this
    }

    def setPort(port: String): this.type = {
      conf += "port" -> port
      this
    }

    def setDatabase(database: String): this.type = {
      conf += "database" -> database
      this
    }

    def setTable(table: String): this.type = {
      conf += "table" -> table
      this
    }

    def read(sc: SparkContext): DataFrame = {
      val sqlContext = SparkSession.builder.getOrCreate().sqlContext
      val mongodbUrl = "mongodb://" + conf("host") + ":" + conf("port") + "/" + conf("database") + "." + conf("table")

      sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> mongodbUrl /*, "partitioner" -> "MongoShardedPartitioner"*/)))
    }

    /** 分区读取 */
    def readByPartition(sc: SparkContext): DataFrame = {
      /** 字段 */
      val fieldTem = Array("")
      val fieldType = Array("")
      val schemaSeq = for ((name, dataType) <- fieldTem.zip(fieldType)) yield {
        StructField(name, CatalystSqlParser.parseDataType(dataType), true)
      }

      val schema = StructType(schemaSeq)

      val partitionType = ""

      //filter  cause
      val query = ""
      //  define function  field  match  data type
      val column = ""
      val mongodbUrl = "mongodb://" + conf("host") + ":" + conf("port") + "/" + conf("database") + "." + conf("table")
      val rdd = partitionType match {
        case "MongoDefaultPartitioner" =>
          // 分区键
          val partitionKey = ""
          val partitionSizeMB = "64 MB"
          Toolkit.toRangeType[Long](partitionSizeMB, 0, Long.MaxValue, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Long.MaxValue}]")
          // 每个分区抽样数
          val samplesPerPartition = "10"
          // 数据验证
          Toolkit.toRangeType[Int](samplesPerPartition, 0, Integer.MAX_VALUE, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Integer.MAX_VALUE}]")
          MongoSpark.load[BsonDocument](sc, ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "DefaultMongoPartitioner",
            "partitionKey" -> partitionKey, "partitionSizeMB" -> partitionSizeMB, "samplesPerPartition" -> samplesPerPartition)))

        case "MongoShardedPartitioner" =>
          val partitionKey = "" // 分区键
          MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "spark.mongodb.input.partitioner" -> "MongoShardedPartitioner",
            "shardKey" -> partitionKey)))
        case "MongoSplitVectorPartitioner" =>
          // 分区键
          val partitionKey = ""
          val partitionSizeMB = "64 MB"
          // 数据验证
          Toolkit.toRangeType[Long](partitionSizeMB, 0, Long.MaxValue, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Long.MaxValue}]")
          MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "MongoSplitVectorPartitioner",
            "partitionKey" -> partitionKey, "partitionSizeMB" -> partitionSizeMB)))
        case "MongoPaginateByCountPartitioner" =>
          // 分区键
          val partitionKey = ""
          // 分区数
          val numberOfPartitions = "10"
          Toolkit.toRangeType[Int](numberOfPartitions, 0, Integer.MAX_VALUE, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Integer.MAX_VALUE}]")
          MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "MongoPaginateByCountPartitioner",
            "partitionKey" -> partitionKey, "numberOfPartitions" -> numberOfPartitions)))

        case "MongoPaginateBySizePartitioner" =>
          val partitionKey = ""
          val partitionSizeMB = "64 MB"
          // 数据验证
          Toolkit.toRangeType[Long](partitionSizeMB, 0, Long.MaxValue, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Long.MaxValue}]")
          MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
            "partitionKey" -> partitionKey, "partitionSizeMB" -> partitionSizeMB)))
      }

      var outDf: DataFrame = null

      // 构造Projection
      val projection = new BsonDocument("$project", BsonDocument.parse(column))
      if (query.isEmpty) {
        val aggregatedRDD = rdd.withPipeline(Seq(projection))
        if (aggregatedRDD.isEmpty())
          outDf = aggregatedRDD.toDF(schema)
        else
          outDf = aggregatedRDD.toDF()
        outDf
      } else {
        val queryMongodb = new bson.Document("$match", BsonDocument.parse(query))
        //val matchQuery = new Document("$match", BsonDocument.parse("{\"type\":\"1\"}"))
        val aggregatedRDD = rdd.withPipeline(Seq(queryMongodb, projection))
        //  val aggregatedRDD = rdd.withPipeline(Seq(projection1))

        if (aggregatedRDD.isEmpty())
          outDf = aggregatedRDD.toDF(schema)
        else
          outDf = aggregatedRDD.toDF()
        outDf
      }
    }

  }


  object JDBC {

    var conf: Map[String, String] = Map("port" -> "27017")

    def setHost(host: String): this.type = {
      conf += "host" -> host
      this
    }

    def setPort(port: String): this.type = {
      conf += "port" -> port
      this
    }

    def setDatabase(database: String): this.type = {
      conf += "database" -> database
      this
    }

    def setTable(table: String): this.type = {
      conf += "table" -> table
      this
    }


//    def read(): Unit = {
//      import java.util.Properties
//
//      import org.apache.spark.sql.SQLContext
//
//      val prop = new Properties()
//      prop.setProperty("user", "")
//      prop.setProperty("password", "")
//
//      prop.setProperty("zeroDateTimeBehavior", "convertToNull") //非法日期值转为null
//
//      prop.setProperty("driver", "com.mysql.jdbc.Driver")
//      prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
//      prop.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
//      prop.setProperty("driver", "com.gbase.jdbc.Driver")
//      prop.setProperty("driver", "org.postgresql.Driver")
//
//      val sql = "select * from user"
//
//      val url = "jdbc:mysql://mysqlHost:3306/database"
//      val sQLContext: SQLContext = null
//
//      // 还可以添加predicates: Array("2016-01-02 00:00:00", "2016-01-02 01:00:00")
//      val comm = sQLContext.read.jdbc(url, "(" + sql + ") TEMPTABLEQQQ", prop)
//      val df = comm.repartition(5)
//      df.cache()
//
//
//      // mpp
//      {
//        import java.util.Properties
//
//        val rddTableName = "<#rddtablename#>"
//
//        val sql = "(select * from tl_testdata_filter) as aa"
//        val prop = new Properties()
//        prop.setProperty("user", "bgetl")
//        prop.setProperty("password", "Bigdata123@")
//        prop.setProperty("driver", "org.postgresql.Driver")
//        val url = "jdbc:postgresql://%s:%d/%s".format("11.39.222.98", 25308, "ods")
//        val newDataFrame = sqlc.read.jdbc(url, sql, prop)
//
//        newDataFrame.cache()
//        outputrdd.put(rddTableName, newDataFrame)
//        newDataFrame.registerTempTable(rddTableName)
//        sqlc.cacheTable(rddTableName)
//
//        newDataFrame.show()
//
//      }
//
//
//      // jdbc
//      def readDataBase(url: String = "jdbc:mysql://192.168.11.26:3306/test",
//                       table: String,
//                       usr: String = "oozie",   // "oozie",
//                       passWord: String = "oozie",
//                       dataBaseType: String = "MySQL"): DataFrame = {
//        val prop = new Properties()
//        prop.setProperty("user", usr)
//        prop.setProperty("password", passWord)
//        dataBaseType.toLowerCase match {
//          case "mysql" => prop.setProperty("driver", "com.mysql.jdbc.Driver")
//          case "oracle" => prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
//          case "sqlserver" => prop.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
//          case "gbase" => prop.setProperty("driver", "com.gbase.jdbc.Driver")
//        }
//        sqlc.read.jdbc(url, table, prop)
//      }

    }


  def getCatalystType(
                       sqlType: Int,
                       precision: Int,
                       scale: Int,
                       signed: Boolean): DataType = {
    val answer = sqlType match {
      case java.sql.Types.ARRAY         => null
      case java.sql.Types.BIGINT        => if (signed) { LongType } else { DecimalType(20,0) }
      case java.sql.Types.BINARY        => BinaryType
      case java.sql.Types.BIT           => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB          => BinaryType
      case java.sql.Types.BOOLEAN       => BooleanType
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.CLOB          => StringType
      case java.sql.Types.DATALINK      => null
      case java.sql.Types.DATE          => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType(precision, scale)

      case java.sql.Types.DECIMAL       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT      => null
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.FLOAT         => FloatType
      case java.sql.Types.INTEGER       => if (signed) { IntegerType } else { LongType }
      case java.sql.Types.JAVA_OBJECT   => null
      case java.sql.Types.LONGNVARCHAR  => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR   => StringType
      case java.sql.Types.NCHAR         => StringType
      case java.sql.Types.NCLOB         => StringType
      case java.sql.Types.NULL          => null

      case java.sql.Types.NUMERIC
        if precision == 0  => DecimalType(38, 0)

      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType(precision, scale) // 还没有和支持的最大最小经度进行比较
      case java.sql.Types.NUMERIC       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR      => StringType
      case java.sql.Types.OTHER         => null
      case java.sql.Types.REAL          => DoubleType
      case java.sql.Types.REF           => StringType
      case java.sql.Types.ROWID         => LongType
      case java.sql.Types.SMALLINT      => IntegerType
      case java.sql.Types.SQLXML        => StringType
      case java.sql.Types.STRUCT        => StringType
      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.TINYINT       => IntegerType
      case java.sql.Types.VARBINARY     => BinaryType
      case java.sql.Types.VARCHAR       => StringType
      case _                            => null
    }
    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }


  def  Struct(url:String,conn:Connection,sql:String) :StructType= {
    val stm = conn.createStatement()
    var sql1 = ""
    var rs: ResultSet = null
    val dialect = JdbcDialects.get(url)
    if (sql.contains("where")) {
      sql1 = sql.replaceAll("where.+", "where 1=0")
    }
    else if (sql.contains("limit")) {
      sql1 = sql.replaceAll("limit.+", "where 1=0")
    }
    else {
      sql1 = sql + " where 1=0"
    }


    rs = stm.executeQuery(sql1)
    val rsmd = rs.getMetaData
    val ncols = rsmd.getColumnCount
    val fields = new Array[StructField](ncols)
    var i = 0
    while (i < ncols) {
      val columnName = rsmd.getColumnLabel(i + 1)
      val dataType = rsmd.getColumnType(i + 1)
      val typeName = rsmd.getColumnTypeName(i + 1)
      val fieldSize = rsmd.getPrecision(i + 1)
      val fieldScale = rsmd.getScale(i + 1)
      val isSigned = rsmd.isSigned(i + 1)
      val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
      val metadata = new MetadataBuilder().putString("name", columnName)
      metadata.putLong("scale",fieldScale)
      val columnType =
        dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
          getCatalystType(dataType, fieldSize, fieldScale, isSigned))
      fields(i) = StructField(columnName, columnType, nullable, metadata.build())
      i = i + 1
    }
    rs.close()
    stm.close()
    conn.close()
    new StructType(fields)
  }



  }


  object HBase {
    def read(): Unit = {
      val tableName = "table"
      val family = ""
      val column = ""
      val sQLContext: SQLContext = null
      val catalogTemplate =
        s"""{
           |"table":{"namespace":"default", "name":"$tableName", "tableCoder":"PrimitiveType"},
           |"rowkey":"key",
           |"columns":{
           |"RowKey":{"cf":"rowkey", "col":"key", "type":"string"},
           |"id":{"cf":"$family", "col":"$column", "type":"string"}
           |}
           |}""".stripMargin

      val hbaseConfig: Configuration = HBaseConfiguration.create()

      val df = sQLContext.read
        .options(Map(HBaseTableCatalog.tableCatalog -> catalogTemplate))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
      df

    }


    def tychyon(): Unit = {

/*      import org.apache.spark.sql.Row
      import org.apache.spark.sql.types._
      val de = System.currentTimeMillis()
      val inputRdd = sc.textFile("alluxio://192.168.11.35:19998/txt/bank.txt").map(s=>Row.fromSeq(s.split(",")))
      println(inputRdd.count())*/

    }

  }


  // 文件读取
  object fileIO {
    def read(): Unit = {
//      FileUtils.readFileToString(new File(pth), Charset.forName(encoding))
    }

    /**
      * 为路径加上user.dir
      * ----
      * 功能: 用于一些resources文件读取, 确保在不同机器运行效果一致
      * ----
      *
      * @param path 路径
      * @return 绝对路径
      */
    def addUserDir2Path(path: String): String = {
      println(s"user.dir为: ${System.getProperty("user.dir")}")
      val userDir = s"${System.getProperty("user.dir")}".replaceAll("\\\\", "\\/")
      if (userDir.endsWith("/") || path.replaceAll("\\\\", "\\/").endsWith("/"))
        userDir + path.replaceAll("\\\\", "\\/")
      else
        userDir + "/" + path.replaceAll("\\\\", "\\/")
    }


  }
