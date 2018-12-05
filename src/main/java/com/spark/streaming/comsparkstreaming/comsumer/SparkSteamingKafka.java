package com.spark.streaming.comsparkstreaming.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Int;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/3
 **/
public class SparkSteamingKafka {
    private static final JavaStreamingContext jsc;

    static {
        SparkConf sparkConf = new SparkConf().setAppName("streaming word count")
                .setMaster("spark://192.168.20.48:7077");
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        sparkConf.set("spark.default.parallelism", "6");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.setLogLevel("WARN");
        jsc = new JavaStreamingContext(javaSparkContext, Durations.seconds(1));
    }

    public static void main(String[] args) {
        SparkSteamingKafka steamingKafka = new SparkSteamingKafka();
        try {
            steamingKafka.wordCount();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void wordCount() throws InterruptedException {
        String brokers = "192.168.20.48:9092";
        String topic = "message";
        Collection<String> topicSet = Arrays.asList(topic.split(","));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("zookeeper.connect", "192.168.20.48:2181,192.168.20.51:2181,192.168.20.52:2181");
        kafkaParams.put("zookeeper.session.timeout.ms", "40000");
        kafkaParams.put("zookeeper.sync.time.ms", "200");
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("group.id", "group1");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //Topic分区
        Map<TopicPartition, Long> offsets = new HashMap<>();
        AtomicInteger partition = new AtomicInteger(0);
        for (String top : topicSet) {
            offsets.put(new TopicPartition(top, 0), 2L);
        }
        //从kafka获取数据
        JavaInputDStream<ConsumerRecord<String, String>> lines = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicSet, kafkaParams, offsets));
        lines.foreachRDD(rdd -> rdd.foreach(System.out::println));

        JavaDStream<String> javaDStream = lines.flatMap((FlatMapFunction<ConsumerRecord<String, String>, String>) record -> Arrays.asList(record.value().trim().split(" ")).iterator());
        JavaPairDStream<String, Integer> javaPairDStream = javaDStream.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> count = javaPairDStream.reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);
        count.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
