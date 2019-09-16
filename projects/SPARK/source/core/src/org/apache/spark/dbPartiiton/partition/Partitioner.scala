package org.apache.spark.sql.dbPartiiton.partition

import java.sql.ResultSet
import java.text.SimpleDateFormat

import de.lmu.ifi.dbs.elki.math.random.XorShift64NonThreadsafeRandom

import scala.util.Random

class Partitioner {

  def  longPartition(lowerBound:Long,upperBound:Long,partiNum:Int):Array[Long]={

    val bounds=new Array[Long](partiNum+1)
    bounds(0)=lowerBound
    bounds(partiNum)=upperBound
    if(partiNum>1) {
      for (i <- 1 until partiNum) {
        bounds(i) =lowerBound+(upperBound-lowerBound+1)*i/partiNum
      }
    }
    bounds
  }

  def  doublePartition(lowerBound:Double,upperBound:Double,partiNum:Int):Array[Double]={

    val bounds=new Array[Double](partiNum+1)
    bounds(0)=lowerBound
    bounds(partiNum)=upperBound
    if(partiNum>1) {
      for (i <- 1 until partiNum) {
        bounds(i) =lowerBound+(upperBound-lowerBound+1)*i/partiNum
      }
    }
    bounds
  }

  //   array  result  desc
  def  longPartiInvesion(lowerBound:Long,upperBound:Long,partiNum:Int):Array[Long]={
     val   bounds=new  Array[Long](partiNum+1)
      bounds(0)=upperBound
      bounds(partiNum)=lowerBound
      if (partiNum>1){
        for (i <- 1 to partiNum - 1) {
          bounds(i) =upperBound-(upperBound-lowerBound+1)*i/partiNum
        }
      }
    bounds
  }


 def  timePartiton(lowerBound:String,upperBound:String,partiNum:Int):Array[String]={

   val  sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

   val  beginTime=sdf.parse(lowerBound)
   val  endTime=sdf.parse(upperBound)
   val  diff=(endTime.getTime-beginTime.getTime)

   var  bounds=new Array[String](partiNum+1)
   bounds(0)="'"+new java.sql.Timestamp(beginTime.getTime)+"'"
   bounds(partiNum)="'"+new java.sql.Timestamp(endTime.getTime)+"'"

   if(partiNum>1) {
     for (i <- 1 until partiNum) {
       bounds(i) = "'"+new java.sql.Timestamp(beginTime.getTime + i * diff / partiNum)+"'"
     }
   }
   bounds

}

  def str2DateLong(str:String,fmt:String): Long ={
    val sdf=new SimpleDateFormat(fmt)
    try{
      val date=sdf.parse(str)
      date.getTime
    }
    catch {
      case e: Exception => //e.printStackTrace()
        0L
    }
  }

  def timePartNoSample(input: ResultSet,timeBegin:String,timeEnd:String,partiNum:Int,fmt:String):Array[String]={
    var partitionNum=partiNum
    if(!input.next())//结果集为空
    {
      partitionNum=1
    }

    val  beginTime=str2DateLong(timeBegin,fmt)
    val  endTime=str2DateLong(timeEnd,fmt)
    val  diff= endTime - beginTime

    var  bounds=new Array[String](partitionNum+1)
    bounds(0)="'"+new java.sql.Timestamp(beginTime)+"'"
    bounds(partitionNum)="'"+new java.sql.Timestamp(endTime)+"'"

    if(partitionNum>1) {
      for (i <- 1 until partitionNum) {
        bounds(i) = "'"+new java.sql.Timestamp(beginTime + i * diff / partitionNum)+"'"
      }
    }
    bounds
  }


  def reservoirSampleLong(
                               input: ResultSet,
                               k: Int,
                               seed: Long = Random.nextLong())
  : Array[Long]= {
    val reservoir = new Array[Long](k)
    var i = 0
    while (i < k && input.next()) {
      val item =input.getLong(1)
      reservoir(i) = item
      i += 1
    }
    if (i < k) {
      val trimReservoir = new Array[Long](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      trimReservoir.sorted
    } else {
      val rand = new XorShift64NonThreadsafeRandom(seed)

      while (input.next()) {
        val item =input.getLong(1)
        val replacementIndex = rand.nextInt(i)
        if (replacementIndex < k) {
          reservoir(replacementIndex)=item
        }
        i += 1
      }
      reservoir.sorted

    }
  }


  def reservoirSampleDouble(
                           input: ResultSet,
                           k: Int,
                           seed: Long = Random.nextLong())
  : Array[Double]= {
    val reservoir = new Array[Double](k)
    var i = 0
    while (i < k && input.next()) {
      val item =input.getDouble(1)
      reservoir(i) = item
      i += 1
    }
    if (i < k) {
      val trimReservoir = new Array[Double](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      trimReservoir.sorted
    } else {
      val rand = new XorShift64NonThreadsafeRandom(seed)

      while (input.next()) {
        val item =input.getDouble(1)
        val replacementIndex = rand.nextInt(i)
        if (replacementIndex < k) {
          reservoir(replacementIndex)=item
        }
        i += 1
      }
      reservoir.sorted
    }
  }




  def reservoirSampleString(
                               input: ResultSet,
                               k: Int,
                               seed: Long = Random.nextLong())
  : Array[String]= {
    val reservoir = new Array[String](k)
    var i = 0
    while (i < k && input.next()) {
      val item =input.getString(1)
      reservoir(i) = item
      i += 1
    }
    if (i < k) {
      val trimReservoir = new Array[String](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      trimReservoir.sorted
    } else {
      val rand = new XorShift64NonThreadsafeRandom(seed)

      while (input.next()) {
        val item =input.getString(1)
        val replacementIndex = rand.nextInt(i)
        if (replacementIndex < k) {
          reservoir(replacementIndex)=item
        }
        i += 1
      }
      reservoir.sorted
    }
  }

  def sampleLongPartition(lowerBound:Long,upperBound:Long,sample:Array[Long],partiNum:Int) :Array[Long]={
    val bounds = new Array[Long](partiNum + 1)
    bounds(0) = lowerBound
    bounds(partiNum) =upperBound
    if (partiNum > 1) {
      if (sample.length > 0) {
        for (i <- 1 to partiNum - 1) {
          val index = (sample.length) * i / partiNum
          bounds(i) =sample(index)
        }
      }
    }
    bounds
  }


def sampleStringPartition(lowerBound:String,upperBound:String,sample:Array[String],partiNum:Int) :Array[String]={
  val bounds = new Array[String](partiNum + 1)
  bounds(0) = "'" + lowerBound + "'"
  bounds(partiNum) = "'" + upperBound + "'"
  if (partiNum > 1) {
    if (sample.length > 0) {
      for (i <- 1 to partiNum - 1) {
        val index = (sample.length) * i / partiNum
        bounds(i) ="'"+sample(index)+"'"
      }
    }
  }
  bounds
}

 //  don't  sample
  def  timeAvePart{}   // need  lower and   upper  reserve
  def  longAvePart{}   // need  lower and   upper  reserve
  def  doubleAvePart{} // need  lower and   upper  reserve



  //  partition with  query  string  condition
  def  stringPart(lowerBound:String,upperBound:String,sample:Array[String],partiNum:Int,indexColumn:String):Array[String]={
    var partitionNum=partiNum
    if(sample.length == 0)
      {
        partitionNum=1
      }

    val bounds = new Array[String](partitionNum)
    val tem=new Array[String](partitionNum-1)

    val tem1 = lowerBound  //preliminary assignment, maybe  be  null
    val tem2 = upperBound    // preliminary assignment, maybe  be  null

    if (partitionNum <= 1) {
      if (tem1.length == 0 && tem2.length > 0) {
        bounds(0) = tem2
      } else if (tem2.length == 0 && tem1.length > 0) {
        bounds(0) = tem1
      } else if (tem2.length > 0 && tem1.length > 0) {
        bounds(0) = lowerBound + " and " + upperBound
      } else {
        bounds(0) = ""
      }
    }
    else{
      if (sample.length > 0) {
        for (i <- 1 to partitionNum - 1) {
          val index = (sample.length)*i/partitionNum
          tem(i-1) ="'"+sample(index)+"'"
        }
        if (tem1.length!=0) {
          bounds(0) =tem1+" and "+indexColumn+"<"+tem(0)
        }else{
          bounds(0)=indexColumn+"<"+tem(0)
        }
        if (tem2.length!=0){
          bounds(partitionNum-1) =indexColumn+">="+tem(partitionNum-2)+" and "+tem2
        }else{
          bounds(partitionNum-1) =indexColumn+">="+tem(partitionNum-2)
        }
        if(partitionNum>2) {
          for (i <- 1 until partitionNum - 1) {
            bounds(i) = indexColumn + ">=" + tem(i-1) + " and " + indexColumn + "<" + tem(i)
          }
        }
      }
    }
    bounds
  }

 //  partition with  query  long  condition
  def  longPart(lowerBound:String,upperBound:String,sample:Array[Long],partiNum:Int,indexColumn:String):Array[String]={
    var partitionNum=partiNum
    if(sample.length == 0)
    {
      partitionNum=1
    }

    val bounds = new Array[String](partitionNum)
    val tem=new Array[Long](partitionNum-1)


    val tem1 = lowerBound  //preliminary assignment, maybe  be  0
    val tem2 = upperBound    // preliminary assignment, maybe  be  0

    if (partitionNum<=1){


      if(tem1.length==0 && tem2.length>0){
        bounds(0)=tem2
      }else if(tem2.length==0 && tem1.length>0){
        bounds(0)=tem1
      }else if (tem2.length>0 && tem1.length>0){
        bounds(0)=lowerBound+" and "+upperBound
      }  else{
        bounds(0)=""
      }


    }else{
      if (sample.length > 0) {
        for (i <- 1 to partitionNum - 1) {
          val index = (sample.length)*i/partitionNum
          tem(i-1) =sample(index)
        }
        if (tem1.length!=0) {
          bounds(0) =tem1+" and "+indexColumn+"<"+tem(0)
        }else{
          bounds(0) =indexColumn+"<"+tem(0)
        }
        if (tem2.length!=0){
          bounds(partitionNum-1) =indexColumn+">="+tem(partitionNum-2)+" and "+tem2
        }else{
          bounds(partitionNum-1) =indexColumn+">="+tem(partitionNum-2)
        }
        if(partitionNum>2) {
          for (i <- 1 to partitionNum - 2) {
            bounds(i) = indexColumn + ">=" + tem(i-1) + " and " + indexColumn + "<" + tem(i)
          }
        }
      }
    }
    bounds
  }

  //  partition with  query  long  condition no sampled
  def  longPartNoSamp(input: ResultSet,lowerBound:String,upperBound:String,partiNum:Int,indexColumn:String):Array[String]={
    var partitionNum=partiNum
//    if(!input.next())
//    {
//      partitionNum=1
//    }

    var low=0L
    var high=0L
    //get lowBound value
    if(lowerBound.contains(">="))
      {
        low=lowerBound.split(">=")(1).trim.toLong
      }
    if(upperBound.contains("<"))
      {
        high=upperBound.split("<")(1).trim.toLong
      }

    if(partitionNum>(high-low) && (high-low)>0) //设置分区数大于实际要分区数
      partitionNum=(high-low).asInstanceOf[Int]
    else if((high-low)<=0)  //实际不能分区
      partitionNum=1
    val sample=new Array[Long](partitionNum+1)
    sample(0)=low
    sample(partitionNum)=high
    if(partitionNum>1) {
        for (i <- 1 until partitionNum) {
          sample(i) = low + (high - low + 1) * i / partitionNum
        }
    }

    val bounds = new Array[String](partitionNum)

    val tem1 = lowerBound  //preliminary assignment, maybe  be  0
    val tem2 = upperBound    // preliminary assignment, maybe  be  0

    if (partitionNum<=1){
      if(tem1.length==0 && tem2.length>0){
        bounds(0)=tem2
      }else if(tem2.length==0 && tem1.length>0){
        bounds(0)=tem1
      }else if (tem2.length>0 && tem1.length>0){
        bounds(0)=lowerBound+" and "+upperBound
      }  else{
        bounds(0)=""
      }
    }else{
      if (sample.length > 0) {
        if (tem1.length!=0) {//有下边界条件
          bounds(0) =tem1+" and "+indexColumn+"<"+sample(1)
        }else{
          bounds(0) =indexColumn+"<"+sample(1)
        }
        if (tem2.length!=0){//有上边界条件
          bounds(partitionNum-1) =indexColumn+">="+sample(partitionNum-1)+" and "+tem2
        }else{
          bounds(partitionNum-1) =indexColumn+">="+sample(partitionNum-1)
        }
        if(partitionNum>2) {
          for (i <- 1 to partitionNum - 2) {
            bounds(i) = indexColumn + ">=" + sample(i) + " and " + indexColumn + "<" + sample(i+1)
          }
        }
      }
    }
    bounds
  }

//  partition with  query  double  condition
  def  doublePart(lowerBound:String,upperBound:String,sample:Array[Double],partiNum:Int,indexColumn:String):Array[String]={
    var partitionNum=partiNum
    if(sample.length == 0)
    {
      partitionNum=1
    }
    val bounds = new Array[String](partitionNum)
    val tem=new Array[Double](partitionNum-1)

    val tem1 = lowerBound  //preliminary assignment, maybe  be  null
    val tem2 = upperBound    // preliminary assignment, maybe  be null

    if (partitionNum<=1){


      if(tem1.length==0 && tem2.length>0){
        bounds(0)=tem2
      }else if(tem2.length==0 && tem1.length>0){
        bounds(0)=tem1
      }else if (tem2.length>0 && tem1.length>0){
        bounds(0)=lowerBound+" and "+upperBound
      }  else{
        bounds(0)=""
      }


    }else{

      if (sample.length > 0) {
        for (i <- 1 to partitionNum - 1) {
          val index = (sample.length)*i/partitionNum
          tem(i-1) =sample(index)
        }
        if (tem1.length > 0) {
          bounds(0) =tem1+" and "+indexColumn+"<"+tem(0)
        }else{
          bounds(0) =indexColumn+"<"+tem(0)
        }
        if (tem2.length > 0){
          bounds(partitionNum-1) =indexColumn+">="+tem(partitionNum-2)+" and "+tem2
        }else{
          bounds(partitionNum-1) =indexColumn+">="+tem(partitionNum-2)
        }
        if(partitionNum>2) {
          for (i <- 1 to partitionNum - 2) {
            bounds(i) = indexColumn + ">=" + tem(i-1) + " and " + indexColumn + "<" + tem(i)
          }
        }
      }

    }
    bounds
  }

  //  partition with  query  double  condition no sampled
  def  doublePartNoSamp(input: ResultSet,lowerBound:String,upperBound:String,partiNum:Int,indexColumn:String):Array[String]={
    var partitionNum=partiNum
    //    if(!input.next())
    //    {
    //      partitionNum=1
    //    }

    var low=0D
    var high =0D
    //get lowBound value
    if(lowerBound.contains(">="))
    {
      low=lowerBound.split(">=")(1).trim.toDouble
    }
    if(upperBound.contains("<"))
    {
      high=upperBound.split("<")(1).trim.toDouble
    }
    if((high-low)<=0)  //实际不能分区
      partitionNum=1

    val sample=new Array[Double](partitionNum+1)
    sample(0)=low
    sample(partitionNum)=high
    if(partitionNum>1) {
      for (i <- 1 until partitionNum) {
        sample(i) =low+(high-low+1)*i/partitionNum
      }
    }

    val bounds = new Array[String](partitionNum)

    val tem1 = lowerBound  //preliminary assignment, maybe  be  0
    val tem2 = upperBound    // preliminary assignment, maybe  be  0

    if (partitionNum<=1){
      if(tem1.length==0 && tem2.length>0){
        bounds(0)=tem2
      }else if(tem2.length==0 && tem1.length>0){
        bounds(0)=tem1
      }else if (tem2.length>0 && tem1.length>0){
        bounds(0)=lowerBound+" and "+upperBound
      }  else{
        bounds(0)=""
      }
    }else{
      if (sample.length > 0) {
        if (tem1.length!=0) {//有下边界条件
          bounds(0) =tem1+" and "+indexColumn+"<"+sample(1)
        }else{
          bounds(0) =indexColumn+"<"+sample(1)
        }
        if (tem2.length!=0){//有上边界条件
          bounds(partitionNum-1) =indexColumn+">="+sample(partitionNum-1)+" and "+tem2
        }else{
          bounds(partitionNum-1) =indexColumn+">="+sample(partitionNum-1)
        }
        if(partitionNum>2) {
          for (i <- 1 to partitionNum - 2) {
            bounds(i) = indexColumn + ">=" + sample(i) + " and " + indexColumn + "<" + sample(i+1)
          }
        }
      }
    }
    bounds
  }


}


