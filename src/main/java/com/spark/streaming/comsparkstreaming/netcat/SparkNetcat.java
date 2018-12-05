package com.spark.streaming.comsparkstreaming.netcat;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Description
 * @Author hasee
 * @Date 2018/11/30
 **/
public class SparkNetcat {
    private static final String host = "192.168.20.48";
    private static final Integer port = 9999;

//    private static final SparkSession sparkSession;

    private static JavaStreamingContext javaStreamingContext;

    static {
        SparkConf sparkConf = new SparkConf()
                .setMaster("spark://192.168.20.48:7077")
                .setAppName("streaming word");
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        sparkConf.set("spark.default.parallelism", "6");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
//        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("error");
        javaStreamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(1));

    }

    public static void main(String[] args) {
        SparkNetcat sparkNetcat = new SparkNetcat();
        try {
            sparkNetcat.getNetcatData();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private void getNetcatData() throws InterruptedException {
        //创建一个将要连接到hostname:port 的离散流
        JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream(host, port);
        JavaDStream<String> javaDStream = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> javaPairDStream = javaDStream.mapToPair(x -> new Tuple2<>(x, 1));
        JavaPairDStream<String, Integer> count = javaPairDStream.reduceByKey((x1, x2) -> x1 + x2);
        // 在控制台打印出在这个离散流（DStream）中生成的每个 RDD 的前十个元素
        count.print();
        //启动计算
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
