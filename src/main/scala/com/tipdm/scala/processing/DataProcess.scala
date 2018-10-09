package com.tipdm.scala.processing

import com.tipdm.scala.utils.SparkUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object DataProcess {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("media_index_process")
    val sc=new SparkContext(conf)
    val sqlContext=new HiveContext(sc)
    //media_index_process
    val origin_media_index_table=args(0)
    val process_media_index_table=args(1)
    //读取Hive中的media_index数据
    val media_index=sqlContext.sql("select * from "+origin_media_index_table)
    val table="tmp_"+System.currentTimeMillis()
    media_index.registerTempTable(table)
    /**
      *  1.删除owner_name=EA级，EB级，EC级，ED级，EE级
      *  2.删除owner_code=02,09,10
      *  3.duration>4000 且duration<21600000
      *  4.res_type!=0 or origin_time not rlike '00$' or end_time not rlike '00$'
      */
    val new_media_index=sqlContext.sql("select * from "+table+" where owner_name!='EA级' or owner_name!='EB级' or owner_name!='EC级' or owner_name!='ED级' or owner_code!='EE级' and owner_code!='02' and owner_code!='09' and owner_code!='10' and duration>4000 and duration<21600000")
    val table1="tmp_"+System.currentTimeMillis()
    new_media_index.registerTempTable(table1)
    val new1_media_index=sqlContext.sql("select * from "+table1+" where res_type!=0 or origin_time not rlike '00$' or end_time not rlike '00$'")
    SparkUtils.saveDF2Hive(sqlContext,new1_media_index,process_media_index_table)
    /*
     * mediamatch_userevent数据预处理
     *  1.删除owner_name=EA级，EB级，EC级，ED级，EE级
     *  2.删除owner_code=02,09,10
     * */
    val origin_mediamatch_userevent_table=args(2)
    val process_mediamatch_userevent_table=args(3)
    val mediamatch_userevent=sqlContext.sql("select * from "+origin_mediamatch_userevent_table)
    val table2="tmp_"+System.currentTimeMillis()
    mediamatch_userevent.registerTempTable(table2)

    val new_mediamatch_userevent=sqlContext.sql("select * from "+table2+" where owner_name!='EA级' or owner_name!='EB级' or owner_name!='EC级' or owner_name!='ED级' or owner_code!='EE级' " +
      "and owner_code!='02' and owner_code!='09' and owner_code!='10' and (run_name='正常' or run_name='主动暂停' or run_name='欠费暂停' or run_name='主动销户')" )
    SparkUtils.saveDF2Hive(sqlContext,new_mediamatch_userevent,process_mediamatch_userevent_table)
    /**
      *  mediamatch_usermsg数据处理
      *  1.删除owner_name=EA级，EB级，EC级，ED级，EE级
      *  2.删除owner_code=02,09,10
      *  3.选择run_name=正常或主动暂停或欠费暂停或主动销户的数据
      */
    val original_mediamatch_usermsg_table=args(4)
    val process_mediamatch_usermsg_table=args(5)
    val mediamatch_usermsg=sqlContext.sql("select * from "+original_mediamatch_usermsg_table)
    val table3="tmp_"+System.currentTimeMillis()
    mediamatch_usermsg.registerTempTable(table3)
    val new_mediamatch_usermsg=sqlContext.sql("select * from "+table3+" where owner_name!='EA级' or owner_name!='EB级' or owner_name!='EC级' or owner_name!='ED级' or owner_code!='EE级' " +
      "and owner_code!='02' and owner_code!='09' and owner_code!='10' and (run_name='正常' or run_name='主动暂停' or run_name='欠费暂停' or run_name='主动销户')" )
    SparkUtils.saveDF2Hive(sqlContext,new_mediamatch_usermsg,process_mediamatch_usermsg_table)
    /**
      * mmconsume_billevents数据预处理
      *  1.删除owner_name=EA级，EB级，EC级，ED级，EE级
      *  2.删除owner_code=02,09,10
      */
    val original_mmconsume_billevents_table=args(6)
    val process_mmconsume_billevents_table=args(7)
    val mmconsume_billevent=sqlContext.sql("select * from "+original_mmconsume_billevents_table)
    val table4="tmp_"+System.currentTimeMillis()
    mmconsume_billevent.registerTempTable(table4)
    val new_mmconsume_billevents=sqlContext.sql("select * from "+table4+" where owner_name!='EA级' or owner_name!='EB级' or owner_name!='EC级' or owner_name!='ED级' or owner_code!='EE级' " +
      "and owner_code!='02' and owner_code!='09' and owner_code!='10'")
    SparkUtils.saveDF2Hive(sqlContext,new_mmconsume_billevents,process_mmconsume_billevents_table)
    /**
      * order_index数据预处理
      *  1.删除owner_name=EA级，EB级，EC级，ED级，EE级
      *  2.删除owner_code=02,09,10
      */
    val original_order_index_table=args(8)
    val process_order_index_table=args(9)
    val order_index_v3=sqlContext.sql("select * from "+original_order_index_table)
    val table5="tmp_"+System.currentTimeMillis()
    order_index_v3.registerTempTable(table5)
    val new_order_index_v3=sqlContext.sql("select * from "+table5+" where owner_name!='EA级' or owner_name!='EB级' or owner_name!='EC级' or owner_name!='ED级' or owner_code!='EE级' " +
      "and owner_code!='02' and owner_code!='09' and owner_code!='10'")
    SparkUtils.saveDF2Hive(sqlContext,new_order_index_v3,process_order_index_table)
    sc.stop()
  }
}
