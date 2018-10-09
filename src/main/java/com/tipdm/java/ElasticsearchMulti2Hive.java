package com.tipdm.java;

import org.apache.spark.deploy.rest.SparkEngine;

import java.io.IOException;

/**
 * //@Author:qwm
 */

public class ElasticsearchMulti2Hive {
    private static String  className="com.tipdm.scala.datasource.ElasticsearchMulti2Hive";
    private static  String appName = "ElasticsearchMulti2Hive";
    public static void main(String[] args) throws IOException, InterruptedException {
        String[] arguments =new String [10];
        arguments[0]="media_index_3m";
        arguments[1]="terminal_no,phone_no,duration,station_name,origin_time,end_time,owner_code,owner_name,vod_cat_tags,resolution,audio_lang,region,res_name,res_type,vod_title,category_name,program_title,sm_name,first_show_time";
        arguments[2]="origin_time";
        arguments[3]="yyyy-MM-dd HH:mm:ss";
        arguments[4]="3";
        arguments[5]="M";
        arguments[6]="yyyyww";
        arguments[7]="media_index";
        arguments[8]="media";
        arguments[9]="2018-08-01 00:00:00";
        String appId = SparkEngine.submit(appName,className,arguments);
        SparkEngine.monitory(appId);
        System.out.println("任务运行成功");
    }
}
