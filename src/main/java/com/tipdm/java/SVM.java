package com.tipdm.java;

import org.apache.spark.deploy.rest.SparkEngine;

import java.io.IOException;

/**
 * //@Author:qwm
 */

public class SVM {
    private static String  className="com.tipdm.scala.svm.SVM";
    private static  String appName = "SVM";
    public static void main(String[] args) throws IOException, InterruptedException {
        String[] arguments =new String [7];
        arguments[0]="mmconsume_billevent_process";
        arguments[1]="mediamatch_userevent_process";
        arguments[2]="media_index_3m_process";
        arguments[3]="mediamatch_usermsg_process";
        arguments[4]="order_index_process";
        arguments[5]="svm_activate";
        arguments[6]="svm_prediction";
        String appId = SparkEngine.submit(appName,className,arguments);
        SparkEngine.monitory(appId);
        System.out.println("任务运行成功");
    }
}
