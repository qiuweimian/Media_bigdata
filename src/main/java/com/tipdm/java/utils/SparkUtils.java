package com.tipdm.java.utils;

import com.tipdm.java.exception.AlgorithmException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.rest.SparkEngine;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Spark / Hadoop 工具类
 * 修改Hadoop Configuration 的获取方式，改为addResource的方式
 * <p>
 * Created by fansy on 2017/2/4.
 */
public class SparkUtils {
    private final static Logger logger = LoggerFactory.getLogger(SparkUtils.class);
    private static final String MODEL_INFORMATION_SUBFIX = "/model_information/part-00000";
    public static int SPARK_SUBMIT_TIMEOUT = 50;// Spark提交任务返回JobID的超时时间，默认50秒，可以通过hadoop.properties设置
    public static int SPARK_JOB_CHECK_INTERVAL = 5;
    private static Configuration conf = null;
    private static FileSystem fs = null;
    private static YarnClient client = null;
    private static String TMP_HDFS_PATH = "/tmp/";
    private static String HIVE_WAREHOUSE = "/user/hive/warehouse";


//    private static WorkFlowWebSocketHandler workFlowWebSocketHandler = SpringUtils.getBean("workFlowWebSocketHandler", WorkFlowWebSocketHandler.class);

    private static void init() {
        TMP_HDFS_PATH = SparkUtilsHelper.getValue("tmp_hdfs_path", SparkUtilsHelperType.HADOOP);
        HIVE_WAREHOUSE = SparkUtilsHelper.getValue("hive.warehouse", SparkUtilsHelperType.HIVE);
        SPARK_SUBMIT_TIMEOUT = Integer.parseInt(
                SparkUtilsHelper.getValue("spark.submit.timeout", SparkUtilsHelperType.HADOOP));
        SPARK_JOB_CHECK_INTERVAL = Integer.parseInt(
                SparkUtilsHelper.getValue("spark.job.check.interval", SparkUtilsHelperType.HADOOP));
        System.setProperty("HADOOP_USER_NAME", SparkUtilsHelper.getValue("yarn.submit.user", SparkUtilsHelperType.HADOOP));
    }

    /**
     * 调用spark 算法，引擎可以使用yarn或spark
     *
     * @param appName
     * @param className
     * @param classArgs
     * @param customJar
     * @return
     */
    public static String runSpark(String appName, String className, String[] classArgs, String... customJar) {
        String appId = null;
        if ("yarn".equals(SparkUtilsHelper.getSparkJobEngine())) {
            // yarn engine
            String[] args = constructArgs(appName, className, classArgs, customJar);
            return runSpark(args);
        } else if ("spark".equals(SparkUtilsHelper.getSparkJobEngine())) {
            // spark engine
            return SparkEngine.submit(appName, className, classArgs);
        } else {
            String errorMsg = "错误的任务引擎，请检查hadoop.properties 中job.engine 配置，只能是'spark'或'yarn'！";
            logger.error(errorMsg);
            throw new AlgorithmException(errorMsg);
        }
//        return appId;
    }


    /**
     * 获取Configuration
     *
     * @return
     */
    public static Configuration getConf() {
        try {
            if (conf == null) {
                init();
                conf = new Configuration();
                conf.set("mapreduce.app-submission.cross-platform",
                        SparkUtilsHelper.getValue("mapreduce.app-submission.cross-platform",
                                SparkUtilsHelperType.HADOOP));// 配置使用跨平台提交任务
                // @TODO make sure the path is right
                File file = new File(SparkUtilsHelper.getRealPathFromSlish(SparkUtilsHelper.PLATFORM_PREFIX + SparkUtilsHelper.getPLATFORM()));
                if (!file.exists() || !file.isDirectory()) {
                    logger.error("路径{}不是目录或不存在！", file.getAbsolutePath());
                    conf = null;//
                    return conf;
                }
                for (File f : file.listFiles()) {
                    if (f.getAbsolutePath().lastIndexOf("xml") != -1) {
                        logger.info("添加{}资源！", f.getName());

                        conf.addResource(f.toURI().toURL());
                    } else {
                        logger.info("资源{}不是以xml结尾！", f.getAbsolutePath());
                    }
                }
                /**
                 * CDH 集群远程提交Spark任务到YARN集群，出现
                 * java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration
                 * 异常，需要设置mapreduce.application.classpath 参数 或
                 * yarn.application.classpath 参数
                 */
                conf.set("yarn.application.classpath",
                        SparkUtilsHelper.getValue("spark.yarn.appMasterEnv.SPARK_DIST_CLASSPATH",
                                SparkUtilsHelperType.SPARK));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conf;
    }


    public static FileSystem getFs() {
        if (fs == null) {
            try {
                fs = FileSystem.get(getConf());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fs;
    }

    /**
     * 获取Client ,用于获取Spark任务的状态
     *
     * @return
     */
    public static YarnClient getClient() {
        if (client == null) {
            client = YarnClient.createYarnClient();
            client.init(getConf());
            client.start(); // TODO start后，等待Tomcat关闭的时候才stop
        }
        return client;
    }

    /**
     * 调用Spark算法
     *
     * @param args
     * @return
     * @deprecated reason this method is deprecated </br>
     * {will be removed in next version} </br>
     * use {@link #runSpark(String, String, String[], String...)}  instead
     */
    @Deprecated
    public static String runSpark(String[] args) {

        StringBuffer buff = new StringBuffer();

        for (String arg : args) {
            buff.append(arg).append(",");
        }
        logger.info("runSpark args:" + buff.toString());
        ApplicationId appId = null;
        try {
            System.setProperty("SPARK_YARN_MODE", "true");
            SparkConf sparkConf = SparkUtilsHelper.getSparkConf(SparkUtilsHelperType.SPARK);
            ClientArguments cArgs = new ClientArguments(args, sparkConf);
            Client client = new Client(cArgs, SparkUtils.getConf(), sparkConf);
//             client.run(); // 去掉此种调用方式，改为有下面的调用方式，这样可以返回jobID
            // 调用Spark
            try {
                appId = client.submitApplication();
            } catch (Throwable e) {
                e.printStackTrace();
                // 清空临时文件
                if (appId == null) {
                    logger.info("提交任务异常，{}", e);
                } else {
                    cleanupStagingDir(appId);
                }
                //  返回null
                return null;
            }
            return appId.toString();
        } catch (Exception e) {
            e.printStackTrace();
            // 清空临时文件
            if (appId != null) {
                cleanupStagingDir(appId);
            }
            logger.error("任务提交异常！，{}", e);
            return null;
        }// 加finally才会异常
// finally{
////            cleanupStagingDir(appId);
//        }
    }

    /**
     * 参考Spark实现删除相关文件代码
     * <p>
     * TODO Tomcat关闭时，如果还有Spark程序还在运行，那么在异常发生时，删除不了文件
     *
     * @param appId
     */
    public static void cleanupStagingDir(ApplicationId appId) {
        String appStagingDir = Client.SPARK_STAGING() + Path.SEPARATOR + appId.toString();

        try {
            Path stagingDirPath = new Path(appStagingDir);
            FileSystem fs = SparkUtils.getFs();
            if (fs.exists(stagingDirPath)) {
                logger.info("Deleting staging directory " + stagingDirPath);
//                fs.delete(stagingDirPath, true);
            }
        } catch (IOException e) {
            logger.warn("Failed to cleanup staging dir " + appStagingDir, e);
        }
    }

    public static void cleanupStagingDir(final String appId) {
        Runnable runnable = () -> {
            try {
                logger.info("准备清空Spark任务：{} 中间日志，10分钟后开始...", appId);
                Thread.sleep(10 * 60 * 1000);// wait for 10 minutes
                cleanupStagingDir(ConverterUtils
                        .toApplicationId(appId));
            } catch (Exception e) {
                logger.error("运行删除spark中间日志任务失败！");
            }
            logger.info("清空Spark任务：{} 中间日志完成", appId);
        };
        new Thread(runnable).start();

    }

    /**
     * @param classArgs
     * @return
     * @deprecated reason this method is deprecated </br>
     * {will be removed in next version} </br>
     * 根据类参数构造Spark提交任务参数
     */
    @Deprecated
    public static String[] constructArgs(String appName, String className, String[] classArgs, String... customJar) {
        boolean hasCustomJar = (customJar != null && customJar.length > 0);
        int defaultSize = 16;
        int defaultClassArgsLength = classArgs.length * 2;

        if (hasCustomJar) {
            defaultSize += 2;
        }
        List<String> argsList = new ArrayList<String>() {{
            add("--name");
            add(appName);
            add("--class");
            add(className);
            add("--driver-memory");
            add(SparkUtilsHelper.getValue("spark.driver.memory", SparkUtilsHelperType.SPARK));
            add("--num-executors");
            add(SparkUtilsHelper.getValue("spark.num.executors", SparkUtilsHelperType.SPARK));
            add("--executor-memory");
            add(SparkUtilsHelper.getValue("spark.executor.memory", SparkUtilsHelperType.SPARK));
            add("--jar");
            add(SparkUtilsHelper.getValue("spark.jar", SparkUtilsHelperType.SPARK));
            add("--files");
            add(SparkUtilsHelper.getValue("spark.files", SparkUtilsHelperType.SPARK));
            add("--executor-cores");
            add(SparkUtilsHelper.getValue("spark.executor.cores", SparkUtilsHelperType.SPARK));
        }};

        if (hasCustomJar) {
            argsList.add("--addJars");
            argsList.add(customJar[0]);
        }

        int j = defaultSize;
        for (int i = 0; i < defaultClassArgsLength / 2; i++) {
            argsList.add("--arg");
            argsList.add(classArgs[i]);
        }
        return argsList.toArray(new String[0]);
    }


    /**
     * 当任务运行成功或失败或被杀死，则返回true（不需要再次检查）
     * 如果是Running状态，则还需要再次检查任务状态
     *
     * @param jobIdStr
     * @return
     */
    private static FinalApplicationStatus getFinalStatus(String jobIdStr) throws IOException, YarnException {
        ApplicationId jobId = ConverterUtils.toApplicationId(jobIdStr);
        ApplicationReport appReport = null;
        try {
            appReport = getClient().getApplicationReport(jobId);
            return appReport.getFinalApplicationStatus();
        } catch (YarnException | IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * 临时HDFS路径
     *
     * @return
     */
    public static String getTempHDFSPath() {
        return TMP_HDFS_PATH + UUID.randomUUID();
    }

    /**
     * 获取Hive数据存储位置
     *
     * @return
     */
    public static String getHiveWarehousePath() {
        return HIVE_WAREHOUSE + "/";
    }

    /**
     * 拷贝HDFS文件
     *
     * @param source
     * @param dest
     */
    public static void copy(String source, String dest) throws IOException {
        FileUtil.copy(getFs(), new Path(source), getFs(), new Path(dest), false, true, getConf());
    }




    /**
     * 按照行数读取txt文件，并返回字符串
     *
     * @param input
     * @param lines
     * @param splitter : 每行数据的分隔符
     * @return
     * @throws Exception
     */
    public static String readTxt(String input, long lines, String splitter) throws IOException {
        StringBuffer buff = new StringBuffer();
        Path path = new Path(input);
        InputStream in = null;
        try {
            FileSystem fs = getFs();
            in = fs.open(path);
            BufferedReader read = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            String line = null;

            while ((line = read.readLine()) != null && lines-- > 0) {
                buff.append(line).append(splitter);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw e;// 抛出此异常即可
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
//				throw e;// 此异常不需要抛出
            }
        }
        return buff.toString();
    }

    /**
     * 写入字符串到HDFS文件中，写入一行
     *
     * @param file
     * @param info
     * @return
     */
    public static boolean writeInfo2HDFS(String file, String info) {
        try {
            FileSystem fs = getFs();
            FSDataOutputStream os = fs.create(new Path(file));
            os.writeUTF(info);
            os.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }



    private static void monitorYarn(String result) throws AlgorithmException {
        boolean shouldReCheck = true;
        while (shouldReCheck) {
            try {
                Thread.sleep(SparkUtils.SPARK_JOB_CHECK_INTERVAL * 1000);
                FinalApplicationStatus applicationStatus = getFinalStatus(result);
                switch (applicationStatus) {
                    case SUCCEEDED:
                        logger.info("=== " + result + " 成功运行并完成!");
                        shouldReCheck = false;
                        break;
                    case FAILED:
                        logger.warn("=== " + result + " 运行异常!");
                        SparkUtils.cleanupStagingDir(result);
                        throw new AlgorithmException("任务运行异常!");
                    case KILLED:
                        logger.warn("=== " + result + " 任务被杀死!");
                        SparkUtils.cleanupStagingDir(result);
                        throw new AlgorithmException("任务被杀死!");
                    case UNDEFINED: // 继续检查
                        break;
                    default:
                        logger.error("=== " + result + "任务状态获取异常!");
                        SparkUtils.cleanupStagingDir(result);
                        throw new AlgorithmException("任务状态获取异常!");
                }
            } catch (InterruptedException | YarnException | IOException e) {
                e.printStackTrace();
                throw new AlgorithmException(e);
            }
        }
    }

    /**
     * 监控Spark任务
     *
     * @param result
     */
    public static void monitor(String result) throws AlgorithmException {
        try {
            if ("yarn".equals(SparkUtilsHelper.getSparkJobEngine())) {
                monitorYarn(result);
            } else if ("spark".equals(SparkUtilsHelper.getSparkJobEngine())) {
                SparkEngineUtils.monitory(result);
            } else {
                String errorMsg = "错误的任务引擎，请检查hadoop.properties 中job.engine 配置，只能是'spark'或'yarn'！";
                logger.error(errorMsg);
                throw new AlgorithmException(errorMsg);
            }
        } catch (AlgorithmException e) {
            throw e;
        }
    }

}
