package com.tipdm.java.labels;

import com.tipdm.java.utils.SparkUtils;
import org.apache.spark.deploy.rest.SparkEngine;

/**
 * Created by ch on 2018/9/19
 */
public class AreaStatistics {
    private final static String className="com.tipdm.scala.statistics.AreaStatistics";
    private final static String appName ="User Area Count";
//    private final static String inputTable="mediamatch_usermsg_process";
//    private final static String outputTable="user_area_count"
    public static String run(String inputTable,String outputTable){
        String[] arguments =new String [2];
        arguments[0]=inputTable;
        arguments[1]=outputTable;
        return SparkEngine.submit(appName,className,arguments);
    }

    public static void main(String[] args) {
        String appId = AreaStatistics.run("mediamatch_usermsg_process","user_area_count");
        SparkEngine.monitory(appId);
    }
}
