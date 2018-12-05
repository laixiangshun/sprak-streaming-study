package com.spark.streaming.comsparkstreaming.builder;

import com.spark.streaming.comsparkstreaming.config.KafkaProperties;
import com.spark.streaming.comsparkstreaming.config.SparkProperties;
import com.spark.streaming.comsparkstreaming.entity.InboundEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

/**
 * @ClassName KafkaStreamBuilder
 * @Description TODO
 * @Author hasee
 * @Date 2018/11/26
 * @Version 1.0
 **/
public class KafkaStreamBuilder<T extends InboundEvent> {
    private KafkaProperties kafkaProperties;

    private SparkProperties sparkProperties;

    private JavaStreamingContext context;

    public KafkaStreamBuilder() {
    }

    @Autowired
    public KafkaStreamBuilder(JavaStreamingContext context, KafkaProperties kafkaProperties, SparkProperties sparkProperties) {
        this.kafkaProperties = kafkaProperties;
        this.context = context;
        this.sparkProperties = sparkProperties;
    }

    public KafkaStreamBuilder setKafkaProperties(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        return this;
    }

    public KafkaStreamBuilder setSparkProperties(SparkProperties sparkProperties) {
        this.sparkProperties = sparkProperties;
        return this;
    }

    public KafkaStreamBuilder setContext(JavaStreamingContext context) {
        this.context = context;
        return this;
    }

    @SuppressWarnings("unchecked")
    public JavaDStream<T> builder() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("key.deserializer", kafkaProperties.getKeyDeserializer());
        kafkaParams.put("value.deserializer", kafkaProperties.getValueDeserializer());
        kafkaParams.put("bootstrap.servers", kafkaProperties.getBootstrap());
        kafkaParams.put("group.ip", kafkaProperties.getGroupId());
        kafkaParams.put("auto.offset.reset", kafkaProperties.getOffsetReset());
        kafkaParams.put("enable.auto.commit", kafkaProperties.isAutoCommit());

        String topics = kafkaProperties.getTopics();
        Collection<String> topicSet = Arrays.asList(topics.split(","));

        //为每一个主题开一个stream去接收，收完了再把结果union起来
        int streamCount = sparkProperties.getStreamCount();
        List<JavaDStream<ConsumerRecord<String, InboundEvent>>> streamList = new ArrayList<>(streamCount);
        for (int i = 0; i < streamCount; i++) {
            streamList.add(KafkaUtils.createDirectStream(context, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicSet, kafkaParams)));
        }
        JavaDStream<ConsumerRecord<String, InboundEvent>> javaDStream = context.union(streamList.get(0), streamList.subList(1, streamList.size()));
        return javaDStream.map(cs -> (T) cs.value());
    }
}
