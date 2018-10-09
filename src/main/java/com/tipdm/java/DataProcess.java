package com.tipdm.java;

import org.apache.spark.deploy.rest.SparkEngine;

import java.io.IOException;


public class DataProcess {
    private static String  className="com.tipdm.scala.processing.DataProcess";
    private static  String appName = "DataProcess";
    public static void main(String[] args) throws IOException, InterruptedException {
        String[] arguments =new String [10];
        arguments[0]="media_index_3m";
        arguments[1]="media_index_3m_process";
        arguments[2]="mediamatch_userevent";
        arguments[3]="mediamatch_userevent_process";
        arguments[4]="mediamatch_usermsg";
        arguments[5]="mediamatch_usermsg_process";
        arguments[6]="mmconsume_billevents";
        arguments[7]="mmconsume_billevent_process";
        arguments[8]="order_index_v3";
        arguments[9]="order_index_process";
        String appId = SparkEngine.submit(appName,className,arguments);
        SparkEngine.monitory(appId);
        System.out.println("任务运行成功");
    }
}
