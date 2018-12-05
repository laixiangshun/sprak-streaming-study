package com.spark.streaming.comsparkstreaming.config;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.spark.streaming.comsparkstreaming.entity.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.*;

/**
 * spark 配置类
 **/
@Configuration
@EnableConfigurationProperties({SparkProperties.class, KafkaProperties.class, MongodbProperties.class, CassandraProperties.class})
public class SparkServerConfig {

    @Autowired
    private SparkProperties sparkProperties;

    @Autowired
    private CassandraProperties cassandraProperties;

//    @Autowired
//    private KafkaProperties kafkaProperties;
//
//    @Autowired
//    private MongodbProperties mongodbProperties;

    /**
     * spark1
     **/
    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf().setAppName(sparkProperties.getAppName())
                .setMaster(sparkProperties.getMaster());
        sparkConf.set("spark.cassandra.connection.host", cassandraProperties.getHost())
                .set("spark.cassandra.connection.port", cassandraProperties.getPort())
                .set("spark.cassandra.connection.keep_alive_ms", cassandraProperties.getKeepAlivems());
        return sparkConf;
    }

    @Bean
    public SparkContext sparkContext() {
        SparkConf sparkConf = new SparkConf().setAppName(sparkProperties.getAppName())
                .setMaster(sparkProperties.getMaster());
        sparkConf.set("spark.cassandra.connection.host", cassandraProperties.getHost())
                .set("spark.cassandra.connection.port", cassandraProperties.getPort())
                .set("spark.cassandra.connection.keep_alive_ms", cassandraProperties.getKeepAlivems());
        SparkContext sparkContext = new SparkContext(sparkConf);
        return sparkContext;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkProperties.getAppName())
                .setMaster(sparkProperties.getMaster());
        //让streaming任务可以优雅的结束，当把它停止掉的时候，它会执行完当前正在执行的任务
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        sparkConf.set("spark.default.parallelism", "6");
        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        return javaSparkContext;
    }

    //spark2 使用spark session,而且datasetx将spark-cassandra-connector中的CassandraSQLContext类移除
    @Bean
    public CassandraConnector sparkSession() {
        SparkSession sparkSession = SparkSession.builder().appName(sparkProperties.getAppName())
                .master(sparkProperties.getMaster())
                .config("spark.cassandra.connection.host", cassandraProperties.getHost())
                .config("spark.cassandra.connection.port", cassandraProperties.getPort())
                .config("spark.cassandra.connection.keep_alive_ms", cassandraProperties.getKeepAlivems())
                .getOrCreate();
        CassandraConnector cassandraConnector = CassandraConnector.apply(sparkSession.sparkContext().conf());
        return cassandraConnector;
    }

    @Bean
    public JavaStreamingContext javaStreamingContext() {
        //设置每一个批处理的时间，我这里是设置的2秒，通常可以1秒
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf(), Durations.seconds(sparkProperties.getInterval()));
        javaStreamingContext.checkpoint(sparkProperties.getCheckpointPath());
        return javaStreamingContext;
    }

//    @Bean
//    public JavaDStream<ConsumerRecord<String, OrderEvent>> javaDStream() {
//
//        //public JavaDStream<ConsumerRecord<String, Object>> javaDStream() {
//        Map<String, Object> kafkaParams = new HashMap<>();
//
//        kafkaParams.put("key.deserializer", kafkaProperties.getKeyDeserializer());
//        kafkaParams.put("value.deserializer", kafkaProperties.getValueDeserializer());
//
//        kafkaParams.put("bootstrap.servers", kafkaProperties.getBootstrap());
//        kafkaParams.put("group.id", kafkaProperties.getGroupId());
//        kafkaParams.put("auto.offset.reset", kafkaProperties.getOffsetReset());
//        kafkaParams.put("enable.auto.commit", kafkaProperties.isAutoCommit());
//
//        String topics = kafkaProperties.getTopics();
//
//        Collection<String> topicsSet = Arrays.asList(topics.split(","));
//
//        List<JavaDStream<ConsumerRecord<String, OrderEvent>>> streamList = new ArrayList<>(
//                //List<JavaDStream<ConsumerRecord<String, Object>>> streamList = new ArrayList<>(
//                sparkProperties.getStreamCount());
//
//        for (int i = 0; i < sparkProperties.getStreamCount(); i++) {
//            streamList.add(KafkaUtils.createDirectStream(javaStreamingContext(), LocationStrategies.PreferConsistent(),
//                    ConsumerStrategies.Subscribe(topicsSet, kafkaParams))
//                    //	ConsumerStrategies.<String, Object>Subscribe(topicsSet, kafkaParams))
//            );
//        }
//
//        JavaDStream<ConsumerRecord<String, OrderEvent>> directKafkaStream = javaStreamingContext()
//                //	JavaDStream<ConsumerRecord<String, Object>> directKafkaStream = javaStreamingContext()
//                .union(streamList.get(0), streamList.subList(1, streamList.size()));
//
//        return directKafkaStream;
//    }

}
