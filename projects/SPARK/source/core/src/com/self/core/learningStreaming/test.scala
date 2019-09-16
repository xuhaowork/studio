package com.self.core.learningStreaming

import java.io.PrintWriter
import java.net.ServerSocket

import com.self.core.baseApp.myAPP
import org.apache.spark.rdd.RDD

import scala.io.Source

object test extends myAPP{
  override def run(): Unit = {
    def index(length: Int): Int = {
      import java.util.Random
      val rdm = new Random
      rdm.nextInt(length)
    }


    val filename = "F://myStudio/people.txt"
    val lines = Source.fromFile(filename).getLines.toList // 文件内容，按行存储
    val filerow = lines.length // 行数


    // 指定监听某端口，当外部程序请求时建立连接
    //    val listener = new ServerSocket(args(1).toInt) // 根据提供的端口号建立socket
    val listener = new ServerSocket(9999)
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println("Got client connected from: " + socket.getInetAddress)

          val out = new PrintWriter(socket.getOutputStream, true)
          while (true) {
            //            Thread.sleep(args(2).toLong)
            Thread.sleep(1000L)

            // 当该端口接受请求时，随机获取某行数据发送给对方
            val content = lines(index(filerow))
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }

      }.start()

    }

  }
}
