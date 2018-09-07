package com.tipdm.java.utils;

import com.tipdm.java.exception.AlgorithmException;
import org.apache.spark.deploy.rest.RestSubmissionClient;
import org.apache.spark.deploy.rest.SparkEngine;
import org.apache.spark.deploy.rest.SubmissionStatusResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark 执行引擎工具类
 * Created by fansy on 2017/11/22.
 */
public class SparkEngineUtils {
    private static Logger logger = LoggerFactory.getLogger(SparkEngineUtils.class);
    /**
     * 执行spark任务 并返回其执行结果成功或失败
     * 线程阻塞，提交任务时候
     * @param slient
     * @param appName
     * @param className
     * @param classArgs
     * @param customJar
     * @return
     */
    public static boolean runSpark(boolean slient,String appName, String className,String[] classArgs,String... customJar){
        String appId = null;
        if("yarn".equals(SparkUtilsHelper.getSparkJobEngine())) {
            String[] args = SparkUtils.constructArgs(appName, className, classArgs, customJar);
             appId = SparkUtils.runSpark(args);
            if (appId == null || "".equals(appId)) {// 提交任务后，获取不到任务信息，异常
                logger.error("=== 获取不到JobID!");
//                throw new AlgorithmException("任务信息获取异常!");
                return false;
            }

        }else{
            appId = SparkEngine.submit(appName,className,classArgs);
            if (appId == null || "".equals(appId)) {// 提交任务后，获取不到任务信息，异常
                logger.error("=== 获取不到JobID!");
//                throw new AlgorithmException("任务信息获取异常!");
                return false;
            }
        }
        // . 监控，并获取状态
        try {
            SparkUtils.monitor(appId);
        }catch (AlgorithmException e){
            logger.error("任务："+appId+" ,运行失败！");
            return false;
        }
        logger.info("=== "+appId +" 任务结束!");
        return true;
    }

    private static RestSubmissionClient client ;
    public static void monitory(String appId) throws AlgorithmException{
        SubmissionStatusResponse response = null;
        boolean finished =false;
        int i =0;
        while(!finished) {

            response = (SubmissionStatusResponse) client.requestSubmissionStatus(appId, true);
            logger.debug("i:{}\t DriverState :{}",i++,response.driverState());

            if("FINISHED" .equals(response.driverState()) ){
                finished = true;
            }
            if( "ERROR".equals(response.driverState())){
//                finished = true;
                throw new AlgorithmException("任务异常!");
            }
            if( "FAILED".equals(response.driverState())){
//                finished = true ;
                throw new AlgorithmException("任务失败!");
            }
            try {
                Thread.sleep(SparkUtils.SPARK_JOB_CHECK_INTERVAL * 1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
        logger.info("Spark Engine Monitor done!");
    }

    static{
        client = new RestSubmissionClient(SparkUtilsHelper.getValue("spark.master",SparkUtilsHelperType.SPARK_STANDALONE));
    }
}
