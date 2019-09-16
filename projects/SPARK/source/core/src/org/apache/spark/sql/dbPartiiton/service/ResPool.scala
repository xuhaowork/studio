package org.apache.spark.sql.dbPartiiton.service

class ResPool {
  type Closeable = {
    def close:Unit
  }

  def apply(s : Integer) = {
    this.size = s
    this
  }

  private var inited = false;
  def isInited() = {
    synchronized {
      val retVal = this.inited
      this.inited = true
      retVal
    }
  }

  private var size : Integer = 10
  private val pool = new java.util.concurrent.LinkedBlockingQueue[Closeable](size + 1)

  def count() = {
    this.pool.size()
  }

  var myCreator : ()=>Unit = null

  def init(creator: => Closeable) : ResPool = {
    this.myCreator = ()=>{
      if(!this.isInited){
        println("Pool is initing!!!")
        (0 to size -1).foreach{ _ =>
          val con = (creator)
          pool.put(con)
        }
      }
    }

    this.myCreator()
    this
  }

  def getRes() : Closeable = {
   System.out.println("Pool take 1 con!")

    this.pool.take()
  }

  def retRes(con : Closeable) = {
    System.out.println("Pool return 1 con!")
    this.pool.put(con)
  }

  def close(): Unit ={
    this.pool.toArray[java.sql.Connection](new Array[java.sql.Connection](0)).foreach(it => it.close)

    this.pool.toArray.foreach { it =>
      it.asInstanceOf[Closeable].close
    }

    this.pool.clear()
    this.inited = false;
    println("pool closed!!!----------------")
  }
}
