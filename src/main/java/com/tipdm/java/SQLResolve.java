package com.tipdm.java;

import org.apache.spark.JobExecutionStatus;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.deploy.rest.SparkEngine;

/**
 * Created by ch on 2018/8/29
 */
public class SQLResolve {
    private static String  className="com.tipdm.scala.SQLEngine";
    private static  String appName = "SQLResolve";

    public static void main(String[] args) {

        String[] arguments={appName,"select t.phone_no,count(*) as num from mmconsume_billevent_process t group by t.phone_no","output_test2","hive","overwrite"};
        String appId =SparkEngine.submit(appName,className,arguments);
        SparkEngine.monitory(appId);
        System.out.println("任务运行成功");

    }
}
