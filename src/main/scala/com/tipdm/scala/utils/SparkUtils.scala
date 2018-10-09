package com.tipdm.scala.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable.ArrayBuffer

/**
  * //@Author:qwm
  * //@Date: 2018/8/15 15:28
  */
object SparkUtils {
  /**
    * 保存数据到MySQL
    * @param result
    * @param outputTable
    * @param url
    */
  def saveDF2MySQL(result:DataFrame,outputTable:String,url:String,user:String,password:String): Unit ={
    val prop =new Properties()
    prop.setProperty("user",user)
    prop.setProperty("password",password)
    prop.setProperty("driver","com.mysql.jdbc.Driver")
    result.write.mode(SaveMode.Overwrite).jdbc(url,outputTable,prop)
  }

  /**
    * 保存DataFrame到 Hive 表中
    *
    * @param sqlContext
    * @param result
    * @param outputTable
    * @return
    */
  def saveDF2Hive(sqlContext: HiveContext, result: DataFrame, outputTable: String) = {
    val tmpTable = "tmp" + System.currentTimeMillis()
    result.registerTempTable(tmpTable)
    if(exists(sqlContext,"default",outputTable)) {
      sqlContext.sql("drop table "+outputTable)
      sqlContext.sql("create table " + outputTable + " as select * from " + tmpTable)
    }else{
      sqlContext.sql("create table " + outputTable + " as select * from " + tmpTable)
    }

  }
  def saveDF2Hive(sqlContext:HiveContext,result:DataFrame, outputTable: String, outputColumns:String) = {
    val tmpTable ="tmp" + System.currentTimeMillis()
    result.registerTempTable(tmpTable)
    if(exists(sqlContext,"default",outputTable)){
      sqlContext.sql("drop table "+outputTable)
      sqlContext.sql("create table "+ outputTable +" as select "+ outputColumns +" from "+ tmpTable)
    }else{
      sqlContext.sql("create table "+ outputTable +" as select "+ outputColumns +" from "+ tmpTable)
    }

  }

  /**
    * 判断Hive表是否存在
    * @param sqlContext
    * @param db
    * @param table
    * @return
    */

  def exists(sqlContext: HiveContext, db: String, table: String): Boolean =
    sqlContext.sql("show tables in " + db + " ").filter("tableName='" + table + "'").collect.length == 1
  /**
    * 删除Hive表
    * @param sqlContext
    * @param table
    * @return
    */
  def dropTable(sqlContext:HiveContext,table:String)={
    sqlContext.sql("drop table "+table)
  }

  /**
    * 获取之前的时间
    *
    * @param timePattern
    * @param timeRangeValue
    * @param timeRangePattern
    * @return
    */


  def getBeforeTimeStr(timePattern: String, timeRangeValue: Int, timeRangePattern: String,startTime:String): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(timePattern)
    dateFormat.format(getBeforeTime(timePattern, timeRangeValue, timeRangePattern,startTime))
  }

  def getBeforeTime(timePattern: String, timeRangeValue: Int, timeRangePattern: String,startTime:String): Date = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(timePattern)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dateFormat.parse(startTime))
    timeRangePattern match {
      case "Y" => cal.add(Calendar.YEAR, -timeRangeValue)
      case "M" => cal.add(Calendar.MONTH, -timeRangeValue)
      case "D" => cal.add(Calendar.DATE, -timeRangeValue)
      case _ => throw new Exception("timeRangePattern not found! timeRangePattern:" + timeRangePattern)
    }
    cal.getTime
  }
  /**
    * 获取 start 到 end 的每一天
    *
    * @param start
    * @param end
    * @return
    */
  def getRangeDays(start: Date, end: Date): Array[Date] = {
    val calBegin = Calendar.getInstance()
    // 使用给定的 Date 设置此 Calendar 的时间
    calBegin.setTime(start);
    val calEnd = Calendar.getInstance()
    // 使用给定的 Date 设置此 Calendar 的时间
    calEnd.setTime(end)
    val arr = new ArrayBuffer[Date]()
    while (end.after(calBegin.getTime())) {
      arr.append(calBegin.getTime)
      calBegin.add(Calendar.DATE, 1) // day
    }
    arr.toArray
  }
  /**
    * 返回 多个表的表名字
    *
    * @param timePattern
    * @param timeRangeValue
    * @param timeRangePattern
    * @param esIndexPre
    * @param esIndexType
    * @param esType
    * @return
    */
  def getBeforeTimeTableNames(timePattern: String, timeRangeValue: Int, timeRangePattern: String,
                              esIndexPre: String, esIndexType: String, esType: String,startTime:String): List[String] = {
    val start = getBeforeTime(timePattern, timeRangeValue, timeRangePattern,startTime)
    //val end = new Date()
    val df: SimpleDateFormat = new SimpleDateFormat(timePattern)
    val end=df.parse(startTime)
    val dateFormat = new SimpleDateFormat(esIndexType)
    val days = for (d <- getRangeDays(start, end)) yield dateFormat.format(d)
    days.toSet.map((x : String) => esIndexPre + x+"/"+esType).toList
  }


}
