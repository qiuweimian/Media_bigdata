package com.tipdm.java;


import com.google.gson.JsonObject;
import com.tipdm.java.labels.LabelUtil;
import com.tipdm.java.utils.Utils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.spark.JobExecutionStatus;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.deploy.rest.SparkEngine;

import java.io.File;
import java.io.IOException;

/**
 * Created by ch on 2018/8/29
 */
public class SQLResolve {
    private static String  className="com.tipdm.scala.SQLEngine";
    private static  String appName = "SQLResolve";



    public static void main(String[] args) throws IOException, InterruptedException {
        String[] labels = {"消费内容","电视消费水平","宽带消费水平","宽带产品带宽","销售品名称","业务品牌",
                "电视入网程度","宽带入网程度"};
        int flag = 0;
        String[] arguments =new String [5];
        String sql =null;
        for(String label:labels){
            sql =LabelUtil.getLabel(label);
            System.out.println("SQL"+sql);
            if(flag==0){
                arguments[4]="overwrite";
            }else {
                arguments[4]="append";
            }
            arguments[0]="SQL"+label;
            arguments[1]=sql;
            arguments[2]="label1";
            arguments[3]="hive";
            String appId =SparkEngine.submit(appName,className,arguments);
            SparkEngine.monitory(appId);
            flag++;
            Thread.sleep(1000*60*5L);
        }
//        String[] arguments={appName,sql,"label1","hive","overwrite"};
        System.out.println("任务运行成功");
    }

}
