package org.apache.spark.binary

/**
  *
  * Author shifeng
  * Date 2018/11/28
  * Version 1.0
  * Update 
  *
  * Desc
  *
  */
object JsonParam {

  val jsonDirLen =
    """
      |{"RERUNNING":{"nodeName":"读二进制文件数据源_2","preNodes":[],"rerun":"true"},
      |"frameSize":"1200",
      |"isSplitable":"true",
      |"readType":{
      |"dirType":{"manualVal":
      |"D:/TestData/TestFileRead1\nD:/TestData/TestFileRead","value":"MANUAL"},
      |"filterFile":{"fileNameLenVal":[2,30],"value":"FILE_NMAE_LENGTH"},
      |"value":"DIR"},
      |"splitSize":"1000"}
    """.stripMargin

  val jsonSingleFile =
    """
      |{"RERUNNING":{"nodeName":"读二进制文件数据源_1","preNodes":[],"rerun":"true"},
      |"frameSize":"10",
      |"isSplitable":"false",
      |"readType":{"dirType":{"value":"CONTROL"},
      |"filterFile":{"value":"FILE_NMAE_LENGTH"},
      |"singleFile":"hdfs://master01:9000/binary/data1/黑暗进化.txt","value":"SINGLE_FILE"},
      |"splitSize":"102400"}
    """.stripMargin

  val jsonMultiFile=
    """
      |{"RERUNNING":{"nodeName":"读二进制文件数据源_1","preNodes":[],"rerun":"true"},
      |"frameSize":"10",
      |"isSplitable":"true","readType":{"dirType":{"value":"CONTROL"},
      |"filterFile":{"value":"FILE_NMAE_LENGTH"},
      |"mutiFile":[{"path":"D:/TestData/TestFileRead/2F21PtxtFile-7.txt"},
      |{"path":"D:/TestData/TestFileRead/33UKO3WVKETZJ8ZtxtFile-10.data"}],"value":"MULTI_FILE"},
      |"splitSize":"10240"}
    """.stripMargin

  val jsonDirPrfix=
    """
      |{"RERUNNING":{"nodeName":"读二进制文件数据源_1","preNodes":[],"rerun":"true"},
      |"frameSize":"10",
      |"isSplitable":"true",
      |"readType":{"dirType":{
      |"controlVal":[{"name":"hdfs://data14.sh.zzjz.com:8020/data/Cuikunlun",
      |"path":"D:/TestData/TestFileRead"},
      |{"name":"hdfs://data14.sh.zzjz.com:8020/data/Dontouch",
      |"path":"D:/TestData/TestFileRead1"}],
      |"value":"CONTROL"},
      |"filterFile":{"fileTypeVal":"txt,doc","value":"FILE_TYPE"},
      |"value":"DIR"},
      |"splitSize":"10240"}
    """.stripMargin

  val jsonDirKey=
    """
      |{"RERUNNING":{"nodeName":"读二进制文件数据源_1","preNodes":[],"rerun":"true"},
      |"frameSize":"10","isSplitable":"true",
      |"readType":{"dirType":{
      |"controlVal":[
      |{"name":"hdfs://data14.sh.zzjz.com:8020/data/Cuikunlun",
      |"path":"D:/TestData/TestFileRead1"},
      |{"name":"hdfs://data14.sh.zzjz.com:8020/data/Dontouch",
      |"path":"D:/TestData/TestFileRead"}],
      |"value":"CONTROL"},
      |"filterFile":{"fileNameVal":"2","value":"FILE_NAME"},
      |"value":"DIR"},
      |"splitSize":"10240"}
    """.stripMargin

  val jsonDirSeq=
    """
      |{"RERUNNING":{"nodeName":"读二进制文件数据源_1","preNodes":[],"rerun":"true"},
      |"frameSize":"10","isSplitable":"true",
      |"readType":{"dirType":{
      |"controlVal":[{"name":"hdfs://data14.sh.zzjz.com:8020/data/Cuikunlun",
      |"path":"D:/TestData/TestFileRead1"},{"name":"hdfs://data14.sh.zzjz.com:8020/data/Dontouch",
      |"path":"D:/TestData/TestFileRead2"}],
      |"value":"CONTROL"},
      |"filterFile":{"fileNameWithSeqSep":"-|_",
      |"fileNameWithSeqVal":[2,5],
      |"value":"FILE_NAME_SEQUENCE"},
      |"value":"DIR"},"splitSize":"10240"}
    """.stripMargin

  val jsonDirRegex=
    """
      |{"RERUNNING":{"nodeName":"读二进制文件数据源_1","preNodes":[],"rerun":"true"},
      |"frameSize":"10",
      |"isSplitable":"true",
      |"readType":{"dirType":{"controlVal":[
      |{"name":"hdfs://data14.sh.zzjz.com:8020/data/Cuikunlun",
      |"path":"D:/TestData/TestFileRead2"},
      |{"name":"hdfs://data14.sh.zzjz.com:8020/data/Dontouch",
      |"path":"D:/TestData/TestFileRead2"}],
      |"value":"CONTROL"},
      |"filterFile":{"regrexVal":".\\w+[0-3]$",
      |"value":"FILE_NAME_REGREX"},"value":"DIR"},
      |"splitSize":"50000"}
    """.stripMargin



  val jsonDirTime=
    """
      |{"RERUNNING":{"nodeName":"读二进制文件数据源_1","preNodes":[],"rerun":"true"},
      |"frameSize":"10",
      |"isSplitable":"false","readType":{"dirType":
      |{"controlVal":[{"name":"hdfs://data14.sh.zzjz.com:8020/data/Cuikunlun",
      |"path":"D:/TestData/TestFileRead1"},{"name":"hdfs://data14.sh.zzjz.com:8020/data/Dontouch",
      |"path":"D:/TestData/TestFileRead2"}],
      |"value":"CONTROL"},
      |"filterFile":{"fileNameWithTimePattern":"txtFile-{yyyyMMdd}",
      |"fileNameWithTimeVal":["2018-10-02 16:14:01","2018-10-04 16:14:11"],
      |"value":"FILE_NAME_TIME"},"value":"DIR"},
      |"splitSize":"10240"}
    """.stripMargin

  val jsonUpdateDir="""{"RERUNNING":{"nodeName":"读二进制文件数据源_3","preNodes":[],"rerun":"false"},
    |"isSplitable":{"splitSize":"10240","frameSize":"10","value":"true"},
    |"readType":{"dirType":{"manualVal":"D:/TestData/TestFileRead2\nD:/TestData/TestFileRead1","value":"MANUAL"},
    |"filterFile":{"fileNameWithSeqSep1":[{"fileNameWithSeqSep":"-"},
    |{"fileNameWithSeqSep":"_"}],
    |"fileNameWithSeqVal":[2,5],
    |"value":"FILE_NAME_SEQUENCE"},
    |"value":"DIR"}}""".stripMargin


  val jsonUpdateSingleFile=
    """
      |{"RERUNNING":{"nodeName":"读二进制文件数据源_单文件读取","preNodes":[],"rerun":"true"},
      |"isSplitable":{"frameSize":"10","splitSize":"1024000000","value":"true"},
      |"readType":{"dirType":{"value":"CONTROL"},"filterFile":{"value":"FILE_NMAE_LENGTH"},
      |"singleFile":"D:/TestData/TestFileRead/8txtFile-5.data",
      |"value":"SINGLE_FILE"}}
    """.stripMargin





}
