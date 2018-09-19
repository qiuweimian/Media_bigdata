package com.tipdm.scala.statistics

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * Created by ch on 2018/9/19
  */
object AreaStatistics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(" User Area Count")
    val sc:SparkContext = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val inputTable = args(0)
    val outputTable= args(1)
    val df = sqlContext.sql("select * from "+inputTable)
    import sqlContext.implicits._
    val area_df =df.select($"phone_no",when($"addressoj".contains("天河区"),"天河").when($"addressoj".contains("海珠区"),"海珠")
      .when($"addressoj".contains("增城"),"增城").when($"addressoj".contains("花都"),"花都").when($"addressoj".contains("黄埔"),"黄埔")
      .when($"addressoj".contains("越秀"),"越秀").when($"addressoj".contains("南沙"),"南沙").when($"addressoj".contains("荔湾"),"荔湾")
      .when($"addressoj".contains("白云"),"白云").when($"addressoj".contains("从化"),"从化").when($"addressoj".contains("番禺"),"番禺")
      .when($"addressoj".contains("从化"),"从化").when($"addressoj".contains("萝岗"),"萝岗").otherwise("其他").alias("area")).distinct()
    val area_group_df = area_df.groupBy("area").agg(count("phone_no").alias("num"))
    area_group_df.write.mode("overwrite").saveAsTable(outputTable)
    sc.stop()
  }
}
