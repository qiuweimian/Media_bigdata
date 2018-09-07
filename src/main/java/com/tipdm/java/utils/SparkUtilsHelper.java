package com.tipdm.java.utils;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * SparkUtils 工具类辅助类
 * Created by fansy on 2017/11/20.
 */
public class SparkUtilsHelper {
    private final static Logger logger = LoggerFactory.getLogger(SparkUtilsHelper.class);
//    private static final String SPARK_ES_PROPERTIES="spark.es.properties";
    private static final String SPARK_STANDALONE_PROPERTIES="spark.standalone.properties";
    private static final String SPARK_PROPERTIES="spark.properties";
    private static final String HADOOP_PROPERTIES="platform/hadoop.properties";
    private static final String HIVE_PROPERTIES="hive.properties";
    private static final String POSTGRE_PROPERTIES="sysconfig/database.properties";
    public static final String PLATFORM_PREFIX= "platform/";
    private static String PLATFORM= null;

    private static Map<String,String > hadoop_properties = new HashMap<>();
    private static Map<String,String > spark_properties = new HashMap<>();
    private static Map<String,String > hive_properties = new HashMap<>();
//    private static Map<String,String > spark_es_properties = new HashMap<>();
    private static Map<String,String > spark_standalone_properties = new HashMap<>();
    private static Map<String,String > postgre_properties = new HashMap<>();

    /**
     * static 定义在 后面
     */
    static{
        logger.info("updating files ... ");
        updateProperties();
    }

    /**
     * 获取根目录开始的绝对路径
     * @param childPath
     * @return
     */
    public static String getRealPathFromSlish(String childPath){
       return  Thread.currentThread().getContextClassLoader().getResource("/").getPath()+"/" + childPath;
    }

    private static String getSparkPath(){
        return getPropertiesPath(SPARK_PROPERTIES);
    }
    private static String getHivePath(){
        return getPropertiesPath(HIVE_PROPERTIES);
    }
//    private static String getESPath(){
//        return getPropertiesPath(SPARK_ES_PROPERTIES);
//    }
    private static String getSparkStandalonePath(){
        return getPropertiesPath(SPARK_STANDALONE_PROPERTIES);
    }
    private static String getPropertiesPath(String childPath){
        return PLATFORM_PREFIX + hadoop_properties.get("platform")+"/"+childPath;
    }

    /**
     * 初始化配置文件
     */
    private static void updateProperties(){
        if(postgre_properties == null || postgre_properties.size() < 1) {
            postgre_properties = PropertiesUtil.getProperties(POSTGRE_PROPERTIES);
        }
        if(hadoop_properties == null || hadoop_properties.size() < 1) {
            hadoop_properties = PropertiesUtil.getProperties(HADOOP_PROPERTIES);
        }
        if(spark_properties == null || spark_properties.size()<1){
            spark_properties = PropertiesUtil.getProperties(getSparkPath());
        }
        if(hive_properties == null || hive_properties.size()<1){
            hive_properties = PropertiesUtil.getProperties(getHivePath());
        }
//        if(spark_es_properties == null || spark_es_properties.size()<1){
//            spark_es_properties = PropertiesUtil.getProperties(getESPath());
//        }
        if(spark_standalone_properties == null || spark_standalone_properties.size()<1){
            spark_standalone_properties = PropertiesUtil.getProperties(getSparkStandalonePath());
        }
    }

    /**
     * 获取配置文件参数值
     * @param key
     * @param type
     * @return
     */
    public static String getValue(String key, SparkUtilsHelperType type){
        switch (type){
            case SPARK:
                return spark_properties.get(key);
            case HADOOP:
                return hadoop_properties.get(key);
            case HIVE:
                return hive_properties.get(key);
//            case SPARK_ES:
//                return spark_es_properties.get(key) ;
            case Postgre:
                return postgre_properties.get(key);
            case SPARK_STANDALONE:
                return spark_standalone_properties.get(key);
            default:
                  logger.warn("不支持的配置文件：{},key:{}",type,key);
        }
        return null ;
    }

    /**
     * 获取Hadoop平台配置文件所在路径
     * @return
     */
    public static String getPLATFORM() {
        if(PLATFORM == null){
            updateProperties();
            PLATFORM = getValue("platform",SparkUtilsHelperType.HADOOP);
        }
        return PLATFORM;
    }

    public static String getSparkJobEngine(){
        return getValue("job.engine",SparkUtilsHelperType.HADOOP);
    }

    /**
     * 获取SparkConf
     * @return
     */
    public  static SparkConf getSparkConf(SparkUtilsHelperType type){
        SparkConf sparkConf = new SparkConf();
        switch (type){
            case SPARK:
                sparkConf.set("spark.yarn.jar", SparkUtilsHelper.getValue("spark.yarn.jar",SparkUtilsHelperType.SPARK));
                sparkConf.set("spark.yarn.scheduler.heartbeat.interval-ms",
                        SparkUtilsHelper.getValue("spark.yarn.scheduler.heartbeat.interval-ms" ,SparkUtilsHelperType.SPARK));
                sparkConf.set("spark.yarn.appMasterEnv.SPARK_DIST_CLASSPATH", SparkUtilsHelper.getValue("spark.yarn.appMasterEnv.SPARK_DIST_CLASSPATH",SparkUtilsHelperType.SPARK));
                sparkConf.set("spark.driver.extraJavaOptions",SparkUtilsHelper.getValue("spark.driver.extraJavaOptions",SparkUtilsHelperType.SPARK));
                break;
            case SPARK_ES:
                sparkConf.set("spark.master",SparkUtilsHelper.getValue("spark.master",SparkUtilsHelperType.SPARK_ES));
                sparkConf.set("spark.app.name",SparkUtilsHelper.getValue("spark.app.name",SparkUtilsHelperType.SPARK_ES));
                sparkConf.set("spark.executor.memory",SparkUtilsHelper.getValue("spark.executor.memory",SparkUtilsHelperType.SPARK_ES));
                sparkConf.set("spark.cores.max",SparkUtilsHelper.getValue("spark.cores.max",SparkUtilsHelperType.SPARK_ES));
                sparkConf.set("spark.executor.cores",SparkUtilsHelper.getValue("spark.executor.cores",SparkUtilsHelperType.SPARK_ES));
                sparkConf.set("spark.jars",
                        // TODO to make sure this is the right path
                        getRealPathFromSlish(SparkUtilsHelper.getValue("spark.jars",SparkUtilsHelperType.SPARK_ES)));
                break;
            case SPARK_STANDALONE:
                sparkConf.set("spark.master",SparkUtilsHelper.getValue("spark.master",SparkUtilsHelperType.SPARK_STANDALONE));
                sparkConf.set("spark.executor.memory",SparkUtilsHelper.getValue("spark.executor.memory",SparkUtilsHelperType.SPARK_STANDALONE));
                sparkConf.set("spark.cores.max",SparkUtilsHelper.getValue("spark.cores.max",SparkUtilsHelperType.SPARK_STANDALONE));
                sparkConf.set("spark.executor.cores",SparkUtilsHelper.getValue("spark.executor.cores",SparkUtilsHelperType.SPARK_STANDALONE));
//                sparkConf.set("spark.executor.extraClassPath",
//                        SparkUtilsHelper.getValue("spark.executor.extraClassPath",SparkUtilsHelperType.SPARK_STANDALONE));
//                sparkConf.set("spark.driver.extraClassPath",
//                        SparkUtilsHelper.getValue("spark.executor.extraClassPath",SparkUtilsHelperType.SPARK_STANDALONE));
                // the code below should not be change
                sparkConf.set("spark.submit.deployMode","cluster");
                sparkConf.set("spark.driver.supervise","false");
                sparkConf.set("spark.files",SparkUtilsHelper.getValue("spark.files",SparkUtilsHelperType.SPARK_STANDALONE));
                break;
            default:
                    logger.warn("not support type:{}",type.name());
        }

        return sparkConf;
    }
}


