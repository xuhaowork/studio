package org.apache.spark.sql.dbPartiiton.service

import java.sql._
import java.util
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry


class ConnectionService {

  val paths = new util.HashMap[String, String]()
  paths.put("Gbase8t", "com.informix.jdbc.IfxDriver")
  paths.put("Gbase8a", "com.gbase.jdbc.Driver")
  paths.put("Oracle", "oracle.jdbc.driver.OracleDriver")
  paths.put("MySQL", "com.mysql.jdbc.Driver")
  paths.put("SqlServer", "com.microsoft.sqlserver.jdbc.SQLServerDriver") //没有行查询
  paths.put("MPPDB", "org.postgresql.Driver")
  paths.put("Postgre", "org.postgresql.Driver") //org.postgresql.Driver


  def getUrl(DBType: String, Host: String, Port: String, Base: String, Sid: String, client_local: String = "en_us.819", db_local: String = "en_us.819"): String = {
    var url: String = null
    if (DBType == "MySQL") {
      url = String.format("jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=UTF8", Host, Integer.valueOf(Port), Base)
    }
    else if (DBType == "Oracle") {
      url = String.format("jdbc:oracle:thin:@%s:%d:%s", Host, Integer.valueOf(Port), Sid)
    }
    else if (DBType == "SqlServer") {
      url = String.format("jdbc:sqlserver://%s:%d;databaseName=%s", Host, Integer.valueOf(Port), Base)
    }
    else if (DBType == "Gbase8a") {
      url = String.format("jdbc:gbase://%s:%d/%s", Host, Integer.valueOf(Port), Base)
    } else if (DBType == "Gbase8t") {
      //url = String.format("jdbc:informix-sqli://%s:%d/%s:INFORMIXSERVER=%s",Host, Integer.valueOf(Port),Base,Sid) //service name
      url = String.format("jdbc:informix-sqli://%s:%d/%s:INFORMIXSERVER=%s", Host, Integer.valueOf(Port), Base, Sid) + getGbase8tUrl("", "", client_local, db_local)
    } else if (DBType == "Postgre" || DBType == "MPPDB") {
      url = String.format("jdbc:postgresql://%s:%d/%s", Host, Integer.valueOf(Port), Base)
    } else {
      url = ""
      println("url  not  ture")
    }
    url
  }

  def getGbase8tUrl(user: String, pwd: String, client_local: String, db_local: String): String = {
    val builder = new StringBuilder
    builder.append(";")
    if (null != user && "" != user && null != pwd && "" != pwd) {
      builder.append("user=" + user + ";")
      builder.append("password=" + pwd + ";")
    }
    if (null != client_local && "" != client_local) {
      builder.append("CLIENT_LOCALE=" + client_local + ";")
    } else {
      builder.append("CLIENT_LOCALE=en_us.819;")
    }
    if (null != db_local && "" != db_local) {
      builder.append("DB_LOCALE=" + db_local + ";")
    } else {
      builder.append("CLIENT_LOCALE=en_us.819;")
    }
    builder.toString()
  }


  def getConnection(driver: String, url: String, user: String, passw: String): Connection = {

    Class.forName(driver)
    val conn = DriverManager.getConnection(url, user, passw)
    conn
  }

  def getConnector(driver: String, url: String, properties: Properties): () => Connection = {
    () => {
      try {
        if (driver != null) DriverRegistry.register(driver)
      } catch {
        case e: ClassNotFoundException =>
          println(s"Couldn't find class $driver", e)
      }
      DriverManager.getConnection(url, properties)
    }
  }


}
