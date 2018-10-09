package com.tipdm.scala.datasource

import com.tipdm.scala.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * hiveTable:Hive表
  * selectedCols:ES资源同步的列字段名
  * tsColName:ES 资源时间列名称
  * tsColPattern:ES资源时间列格式,如 yyyyMMdd HH:mm:ss
  * timeRangeValue (required)    ES资源同步时间段值
  * timeRangeType (required)    ES资源同步时间段类型, Y|M|D
  * esTable:ES资源名，index/type
  * startTime:同步设置的这个时间之前的数据
  * //@Author:qwm
  */

object ES2Hive {
  val default_query: String = "?q=*:*"
  def main(args: Array[String]): Unit = {
    val hiveTable=args(0)
    val selectedCols=args(1)
    val tsColName=args(2)
    val tsColPattern=args(3)
    val timeRangeValue=args(4).toInt
    val timeRangeType=args(5)
    val esTable=args(6)
    val startTime=args(7)
    //val esTableMulti=args(8).toBoolean
    val options =
        Map(
      ("es.nodes", "192.168.111.75"),
      ("es.port", "9200"),
      ("es.read.metadata", "false"),
      ("es.mapping.date.rich", "false")
    )
    val conf = new SparkConf().setAppName("ES2Hive")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("set spark.sql.caseSensitive=true")
    val sparkTable: String = "tmp" + System.currentTimeMillis()
    val sql = "CREATE TABLE " + hiveTable + " as  select " + selectedCols + " from " + sparkTable +
      " where " + tsColName + " > '" + SparkUtils.getBeforeTimeStr(tsColPattern, timeRangeValue, timeRangeType,startTime) + "'"
    val esDf= sqlContext.esDF(esTable, default_query, options)

    esDf.registerTempTable(sparkTable)
    if(SparkUtils.exists(sqlContext,"default",hiveTable)){
      SparkUtils.dropTable(sqlContext,hiveTable)
      sqlContext.sql(sql)
    }else{
      sqlContext.sql(sql)
    }
    sc.stop()
  }
}
