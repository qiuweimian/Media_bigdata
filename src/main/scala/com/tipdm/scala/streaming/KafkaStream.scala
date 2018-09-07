package com.tipdm.scala.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.tipdm.scala.utils.InternalRedisClient
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulator, SparkConf}
import redis.clients.jedis.Pipeline

/**
  * Created by ch on 2018/8/15
  */
object KafkaStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("kafkaStream")
    conf.setMaster("local[3]")
    val ssc = new StreamingContext(conf,Seconds(30))
    val sqlContext= new SQLContext(ssc.sparkContext)
    //kafka topic
    val topic = "test_topic"
    //topic的分区标记
    val partition =0
    val topics = Set("test_topic")
    val brokers = "node2:9092,node3:9092,node4:9092"
    val kafkaParams:Map[String,String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> "exactly-once",
      "enable.auto.commit" -> "false"
//      "auto.offset.reset" -> "none"
    )


    var sum:Long =0
//    InternalRedisClient.makePool(redisHost, redisPort, redisTimeout,password,dbDefaultIndex, maxTotal, maxIdle, minIdle)
    val jedis = InternalRedisClient.getJedis()
    val topic_partition_key = topic + "_" + partition
    //从redis中获取上次消费到的offset位置，如果没有记录，则默认从0开始消费
    var lastOffset = 0l
    val lastSavedOffset = jedis.get(topic_partition_key)
    if(null != lastSavedOffset) {
      try {
        lastOffset = lastSavedOffset.toLong
      } catch {
        case ex : Exception => println(ex.getMessage)
          println("get lastSavedOffset error, lastSavedOffset from redis [" + lastSavedOffset + "] ")
          System.exit(1)
      }
    }
    val totalCost:Accumulator[Double]=ssc.sparkContext.accumulator(0.0)
    val validOrders:Accumulator[Int] =ssc.sparkContext.accumulator(0)
    val historyTotal = jedis.get("totalcost")
    val historyValidOrders = jedis.get("validOrders")
    val historyOrders = jedis.get("totalOrders")
    jedis.close()
    val increaseValidOrders = ssc.sparkContext.accumulator(0)
    val increaseCost:Accumulator[Double] = ssc.sparkContext.accumulator(0.0)

    if(null!=historyOrders){
      sum=historyOrders.toInt
    }
    if(null!=historyValidOrders){
      validOrders.add(historyValidOrders.toInt)
    }
    if(null != historyTotal){
      totalCost.add(historyTotal.toDouble)
    }
    val fromOffsets:Map[TopicAndPartition, Long] = Map{new TopicAndPartition(topic, partition) -> lastOffset}
    val messageHandler: MessageAndMetadata[String, String] => (String, String) =
    (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

    println("lastOffset from redis -> " + lastOffset)
    val offsetRanges =Array[OffsetRange]()
    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets,messageHandler)
    //在这里处理每一批过来的数据

    kafkaStream.foreachRDD(
      rdd =>{
        val offsetRanges= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //新增订单每一批次重置为0
        increaseValidOrders.setValue(0)
        //新增订单营业额重置为0
        increaseCost.setValue(0.0)
//        println("Partitions: "+rdd.getNumPartitions)
//        val ab =rdd.first()
        //把过来的json数据转变dataframe
        println("key为 "+rdd.first()._1)
        //把过来的json数据转变dataframe
        val data =sqlContext.read.json(rdd.map(_._2))
//        println(data.schema)
        //按业务逻辑处理每一行数据
        data.foreach(row=>{
          val cost = row.getAs[String]("cost")
          //订单有效判断
          if(null == cost||cost.equalsIgnoreCase("null") || cost.startsWith("YH")){

          }else{
            //有效订单统计
            validOrders.add(1)
            increaseValidOrders.add(1)
            //每次总营业额
            increaseCost.add(cost.toDouble)
            //总营业额
            totalCost.add(cost.toDouble)
          }
        })
        //记录总数即订单总数
        sum =sum+rdd.count()
        println("新增订单数: "+rdd.count())
        println("新增有效订单数："+increaseValidOrders)
        println("有效订单总数： "+validOrders)
        println("总订单数: "+sum)
        println("新增订单营业额："+increaseCost)
        println("订单总营业额："+totalCost)
        val jedis = InternalRedisClient.getJedis()
        val p1 : Pipeline = jedis.pipelined()
        p1.multi() //开启事务
        p1.set("totalcost",totalCost.toString())
        //每小时统计一次总营业额，总订单，有效订单保存到redis,key以时间开头(如2018082413_xxx)
        val key =nowTime()
        if(key.endsWith("00")){
          val totalKey= key.substring(0,10)+"_totalcost"
          p1.set(totalKey,totalCost.toString())
          p1.set(key.substring(0,10)+"_totalorders",sum.toString)
          p1.set(key.substring(0,10)+"_validorders",validOrders.toString())
        }
        //每一批次都更新统计指标
        p1.set("increase_cost",increaseCost.toString())
        p1.set("increase_valid_order",increaseValidOrders.toString)
        p1.set("validOrders",validOrders.toString())
        p1.set("totalOrders",sum.toString)
        p1.set("increase_order",rdd.count().toString)
        //保存spark streaming消费kafka的topic的partition中的offset，以便重启后继续从上次的置消费
        offsetRanges.foreach { offsetRange =>
          println("partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
          val topic_partition_key = offsetRange.topic + "_" + offsetRange.partition
          p1.set(topic_partition_key, offsetRange.untilOffset + "")
        }
        p1.exec();//提交事务
        p1.sync();//关闭pipeline
        jedis.close()
      }
  )
    ssc.start()
    ssc.awaitTermination()
  }
  def nowTime():String={
    val now:Date = new Date()
    val dataFormate:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val date = dataFormate.format(now)
    return date
  }
}
