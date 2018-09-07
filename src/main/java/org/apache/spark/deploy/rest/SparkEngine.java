package org.apache.spark.deploy.rest;

import com.tipdm.java.exception.AlgorithmException;
import com.tipdm.java.utils.SparkUtilsHelper;
import com.tipdm.java.utils.SparkUtilsHelperType;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.rest.CreateSubmissionResponse;
import org.apache.spark.deploy.rest.RestSubmissionClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

//import scala.Predef;
//import scala.Tuple2;
//import scala.collection.JavaConverters;

/**
 * Spark 引擎：
 * 1）调用Spark算法,提交任务到Spark StandAlone集群，并返回id；
 * 2）根据id监控Spark 任务；
 * Created by fanzhe on 2018/1/2.
 */
public class SparkEngine {
    static{
        client = new RestSubmissionClient(SparkUtilsHelper.getValue("spark.master",SparkUtilsHelperType.SPARK_STANDALONE));
    }
    private static final Logger log = LoggerFactory.getLogger(SparkEngine.class);
    public static int SPARK_JOB_CHECK_INTERVAL = 5;

    public static String submit(String appName,String mainClass,String ...args){

        log.info("run args:\n"+ Arrays.toString(args));

        SparkConf sparkConf = SparkUtilsHelper.getSparkConf(SparkUtilsHelperType.SPARK_STANDALONE);
        sparkConf.setAppName(appName +" "+ System.currentTimeMillis());

        Map<String,String> env = filterSystemEnvironment(System.getenv());

        CreateSubmissionResponse response = null;
        try {
            response = (CreateSubmissionResponse)
                    RestSubmissionClient.run(SparkUtilsHelper.getValue("spark.appResource",
                            SparkUtilsHelperType.SPARK_STANDALONE), mainClass, args, sparkConf,
//                            toScalaMap(env)
                            new scala.collection.immutable.HashMap()
                    );
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

        return response.submissionId();
    }



    private static Map<String, String> filterSystemEnvironment(Map<String, String> env) {
        Map<String,String> map = new HashMap<>();
        for(Map.Entry<String,String> kv : env.entrySet()){
            if(kv.getKey().startsWith("SPARK_") && kv.getKey() != "SPARK_ENV_LOADED"
                    || kv.getKey().startsWith("MESOS_")){
                map.put(kv.getKey(),kv.getValue());
                log.warn("Environment has something with SPARK_ : "+kv.getKey());
            }
        }
        return map;
    }

    public static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> m) {
//        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
//                Predef.<Tuple2<A, B>>conforms()
//        );
        // TODO this can not be right
        return new scala.collection.immutable.HashMap<A,B>();
    }

    private static RestSubmissionClient client ;
    public static void monitory(String appId) throws AlgorithmException{
        SubmissionStatusResponse response = null;
        boolean finished =false;
        int i =0;
        while(!finished) {

            response = (SubmissionStatusResponse) client.requestSubmissionStatus(appId, true);
//            logger.debug("i:{}\t DriverState :{}",i++,response.driverState());

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
                Thread.sleep(SPARK_JOB_CHECK_INTERVAL * 1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
//        logger.info("Spark Engine Monitor done!");
    }



}
