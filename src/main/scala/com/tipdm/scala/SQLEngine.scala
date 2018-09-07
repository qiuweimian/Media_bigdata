package com.tipdm.scala

import java.util.Properties

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ch on 2018/8/28
  */
object SQLEngine {
  def main(args: Array[String]): Unit = {
    val appName = args(0)
    val sql = args(1)
    val outputTable =args(2)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val data = sqlContext.sql(sql)
    println("**********"+data.count())
    println(data.head().toString())
    val dbType = args(3)
    val saveMode=args(4)
     dbType match {
      case "hive" =>data.write.mode(saveMode).saveAsTable(outputTable)
      case "rdbms"=>{
        val url =args(5)
        val connectionProperties = new Properties()
        connectionProperties.setProperty("user",args(6))
        connectionProperties.setProperty("password",args(7))
        data.write.mode(saveMode).jdbc(url,outputTable,connectionProperties)
      }
    }
    sc.stop()
  }
}
