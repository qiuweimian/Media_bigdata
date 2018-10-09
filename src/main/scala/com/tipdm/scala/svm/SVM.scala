package com.tipdm.scala.svm

import com.tipdm.scala.utils.SparkUtils
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
/*
* //@Author:qwm
* */

object SVM {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("SVM")
    val sc=new SparkContext(conf)
    val sqlContext=new HiveContext(sc)
    //val mmconsume="mmconsume_billevent_process"
    val mmconsume=args(0)
    //val mediamatch_userevents="mediamatch_userevent_process"
    val mediamatch_userevents=args(1)
    //val media_index_t="media_index_3m_process"
    val media_index_t=args(2)
    //val mediamatch_usermsg="mediamatch_usermsg_process"
    val mediamatch_usermsg=args(3)
    //val active="active_label"
    //val order_index="order_index_process"
    val order_index=args(4)
    //电视用户活跃度标签计算
    val msg=sqlContext.sql("select distinct phone_no,0 as col1 from "+mediamatch_usermsg)
    val mediaIndex=sqlContext.sql("select phone_no,sum(duration) as total_one_month_seconds from "+media_index_t+" group by phone_no having total_one_month_seconds>18720000").select("phone_no","total_one_month_seconds")
    val order_index_tv=sqlContext.sql("select * from "+order_index+" where run_name='正常' and offername!='废' and offername!='赠送' and offername!='免费体验' and offername!='提速' and offername!='提价' and offername!='转网优惠' and offername!='测试' and offername!='虚拟' and offername!='空包' and offername not like '%宽带%'").select("phone_no").distinct()
    val active_join2=mediaIndex.join(order_index_tv,Seq("phone_no"),"inner").selectExpr("phone_no","1 as col2").distinct()
    //用户活跃度，col1为1表示为活跃用户，0表示为不活跃用户
    val active_join3=msg.join(active_join2,Seq("phone_no"),"left_outer").na.fill(0).selectExpr("phone_no","col2 as col1")
    //评价结果表
    //val activate_table="svm_activate"
    //val prediction_table="svm_prediction"
    val activate_table=args(5)
    //预测表
    val prediction_table=args(6)
    //构造svm数据
    //统计每个用户的月均消费金额C
    val billevents=sqlContext.sql("select phone_no, sum(should_pay)/3 consume  from "+mmconsume+" where sm_name not like '%珠江宽频%' group by phone_no")

    //统计每个用户的入网时长max(当前时间-run_time)
    val userevents=sqlContext.sql("select phone_no,max(months_between(current_date(),run_time)/12) join_time from "+mediamatch_userevents+" group by phone_no")

    //统计每个用户平均每次看多少小时电视M
    val media_index=sqlContext.sql("select phone_no,(sum(media.duration)/(1000*60*60))/count(1) as count_duration from "+media_index_t+" media group by phone_no")
    val join1=billevents.join(userevents,Seq("phone_no")).join(media_index,Seq("phone_no"))
    //mediamatch_usermsg选出离网的用户（run_name ='主动销户' or run_name='主动暂停' )贴上类别0（离网）；在正常用户中提取有活跃标签的用户贴上类别1（不离网）。
    val usermsg=sqlContext.sql("select *  from "+mediamatch_usermsg+" where  run_name ='主动销户' or run_name='主动暂停' ")
    //给离网用户贴上类别0
    val join2=usermsg.join(join1,Seq("phone_no"),"inner").withColumn("label",join1("consume")*0)
    //在正常用户中提取有活跃标签的用户贴上类别1（不离网）
    val activate_user=active_join3.where("col1=1")
    val join3=join1.join(activate_user,Seq("phone_no"),"inner").withColumn("label",join1("consume")*0+1)
    val union_data=join2.select("phone_no","consume","join_time","count_duration","label").unionAll(join3.select("phone_no","consume","join_time","count_duration","label"))

    //训练数据为union_data
    val traindata = union_data.select("consume","join_time","count_duration").rdd .
      zip( union_data.select("label").rdd).map(x => LabeledPoint(x._2.get(0).toString.toDouble,
      Vectors.dense(x._1.toSeq.toArray.map(_.toString.toDouble))) )

   //测试数据集
    val test_data=join1.select("phone_no").rdd.zip(join1.select("consume","join_time","count_duration").rdd).map(x=>(x._1.get(0).toString,Vectors.dense(x._2.toSeq.toArray.map(_.toString.toDouble))))

    //归一化
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(traindata.map(x => x.features))
    val data2 = traindata.map(x => LabeledPoint(x.label, scaler.transform(Vectors.dense(x.features.toArray))))
    val data2_test = data2.map(x => (x.label, scaler.transform(Vectors.dense(x.features.toArray))))
    //将数据分为训练集和验证集
    val train_validate = data2.randomSplit(Array(0.8,0.2))
    val (train_data, validate_data) = (train_validate(0),train_validate(1))
    //建模
    val model=SVMWithSGD.train(train_data,100,0.01,0.01,1.0)
    //评估
   // model.clearThreshold()

    val predictAndLabel = validate_data.map(row  => {
      val predict = model.predict(row.features)
      val label = row.label
      (predict,label)
    })
    val validateCorrectRate = predictAndLabel.filter(r => r._1 == r._2).count.toDouble / validate_data.count()
    val metrics = new BinaryClassificationMetrics(predictAndLabel)
    val schema1 = StructType(Array(
      StructField("param_original",StringType,false),
      StructField("value",DoubleType,false)))
    val rdd1 = sc.parallelize(Array(
      Row("correctRate",validateCorrectRate),
      Row("areaUnderROC",metrics.areaUnderROC()),
      Row("areaUnderPR",metrics.areaUnderPR())
    ))

    val evaluation=sqlContext.createDataFrame(rdd1,schema1)
   // SparkUtils.saveDF2Hive(sqlContext,evaluation,activate_table)
    SparkUtils.saveDF2MySQL(evaluation,activate_table,"jdbc:mysql://192.168.111.75:3306/zjsm","root","root")


    //预测
    val predict_data = test_data.map(row =>{
      val predict = model.predict(row._2)
      Row(row._1,row._2(0),row._2(1),row._2(2),predict)
    })

    val schema = StructType(Array(StructField("phone_no",StringType,false),StructField("consume",DoubleType,false),StructField("join_time",DoubleType,false),StructField("count_duration",DoubleType,false),StructField("label",DoubleType,false)))
    val predict_df = sqlContext.createDataFrame(predict_data,schema)
  //  SparkUtils.saveDF2Hive(sqlContext,predict_df,prediction_table)
    SparkUtils.saveDF2MySQL(predict_df,prediction_table,"jdbc:mysql://192.168.111.75:3306/zjsm","root","root")
  }

}
