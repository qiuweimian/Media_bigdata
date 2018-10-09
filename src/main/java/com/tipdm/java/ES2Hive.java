package com.tipdm.java;


import org.apache.spark.deploy.rest.SparkEngine;

import java.io.IOException;

/**
 * //@Author:qwm
 * 读取mediamatch_usermsg：参数如下
 * arguments[0]="mediamatch_usermsg";
 * arguments[1]="terminal_no,phone_no,sm_name,run_name,sm_code,owner_name,owner_code,run_time,addressoj,estate_name,open_time,force";
 * arguments[2]="run_time";
 * arguments[3]="yyyy-MM-dd HH:mm:ss";
 * arguments[4]="50";
 * arguments[5]="Y";
 * arguments[6]="mediamatch_usermsg/doc";
 * arguments[7]="2018-08-01 00:00:00";
 * 读取mediamatch_userevent:参数如下
 * arguments[0]="mediamatch_userevent";
 * arguments[1]="phone_no,run_name,run_time,owner_name,owner_code,open_time";
 * arguments[2]="run_time";
 * arguments[3]="yyyy-MM-dd HH:mm:ss";
 * arguments[4]="50";
 * arguments[5]="Y";
 * arguments[6]="mediamatch_userevent/doc";
 * arguments[7]="2018-08-01 00:00:00";
 * 读取mmconsume_billevent：参数如下
 * arguments[0]="mmconsume_billevents";
 * arguments[1]="terminal_no,phone_no,fee_code,year_month,owner_name,owner_code,sm_name,should_pay,favour_fee";
 * arguments[2]="year_month";
 * arguments[3]="yyyy-MM-dd HH:mm:ss";
 * arguments[4]="1";
 * arguments[5]="Y";
 * arguments[6]="mmconsume_billevents/doc";
 * arguments[7]="2018-08-01 00:00:00";
 * 读取order_index：参数如下
 * arguments[0]="order_index_v3";
 * arguments[1]="phone_no,owner_name,optdate,prodname,sm_name,offerid,offername,business_name,owner_code,prodprcid,prodprcname,effdate,expdate,orderdate,cost,mode_time,prodstatus,run_name,orderno,offertype";
 * arguments[2]="optdate";
 * arguments[3]="yyyy-MM-dd HH:mm:ss";
 * arguments[4]="10";
 * arguments[5]="Y";
 * arguments[6]="order_index_v3/doc";
 * arguments[7]="2018-08-01 00:00:00";
 */
public class ES2Hive {
    private static String  className="com.tipdm.scala.datasource.ES2Hive";
    private static  String appName = "ES2Hive";
    public static void main(String[] args) throws IOException, InterruptedException {
        String[] arguments =new String [8];
        arguments[0]="mediamatch_usermsg";
        arguments[1]="terminal_no,phone_no,sm_name,run_name,sm_code,owner_name,owner_code,run_time,addressoj,estate_name,open_time,force";
        arguments[2]="run_time";
        arguments[3]="yyyy-MM-dd HH:mm:ss";
        arguments[4]="50";
        arguments[5]="Y";
        arguments[6]="mediamatch_usermsg/doc";
        arguments[7]="2018-08-01 00:00:00";
        String appId = SparkEngine.submit(appName,className,arguments);
        SparkEngine.monitory(appId);
        System.out.println("任务运行成功");
    }
}
