package com.ly.flink.job.sources

import java.sql.{Connection, DriverManager, SQLException}

import com.ly.flink.consts.CaseClass.{CrimeDoc, Phone}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class PostgreSource extends RichSourceFunction[CrimeDoc]{
  var connection: Connection = _
  override def run(sourceContext: SourceFunction.SourceContext[CrimeDoc]): Unit = {
      val sql = "SELECT * FROM tb_crime "
    val prep = connection.prepareStatement(sql)
    while(true) {
      try {
        val rs = prep.executeQuery()
        while (rs.next()) {
          val doc = CrimeDoc(
            rs.getInt("id"),
            rs.getString("idCard"),
            rs.getString("name"),
            rs.getString("num"),
            rs.getString("detail")
          )
          sourceContext.collect(doc)
        }
      } catch {
        case e: SQLException => println("sql exception: " + e.getCause)
//        case _ => println("创建sql连接失败，未知异常！")
      }
      //一天更新一次广播流
      Thread.sleep(24*3600*1000)
    }

  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val username = "ss"
    val passwd = "ss"
    val url = "jdbc:postgresql://192.168.1.101:5432/db_ss"
    val diver = "org.postgresql.Driver"
    Class.forName(diver)
    try{
      connection = DriverManager.getConnection(url,username,passwd)
    }catch {
      case e: SQLException =>println("sql exception: "+e.getCause)
//      case _ =>println("创建sql连接失败，未知异常！")
    }


  }

  override def close(): Unit = {
    super.close()
  }

  override def cancel(): Unit = {

  }
}
