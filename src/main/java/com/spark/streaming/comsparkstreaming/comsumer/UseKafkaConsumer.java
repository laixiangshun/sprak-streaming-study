package com.spark.streaming.comsparkstreaming.comsumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * kafka 消费者 原生kafka方式
 **/
public class UseKafkaConsumer {
    //    private KafkaConsumer<String, String> consumer;
    private ConsumerConnector consumer;
    private String topic;

    public static void main(String[] args) {
        UseKafkaConsumer kafkaConsumer = new UseKafkaConsumer("message");
        kafkaConsumer.receive();
    }

    private UseKafkaConsumer(String topic) {
        consumer = kafka.consumer.Consumer
                .createJavaConsumerConnector(consumerConfig());
        this.topic = topic;
    }

    private static ConsumerConfig consumerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.20.48:9092");
        props.put("zookeeper.connect", "192.168.20.48:2181,192.168.20.51:2181,192.168.20.52:2181");
        props.put("group.id", "group1");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    private void receive() {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        StringDecoder ketDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<String, String>>> messageStreams = consumer.createMessageStreams(topicCountMap, ketDecoder, valueDecoder);
        KafkaStream<String, String> kafkaStream = messageStreams.get(topic).get(0);
        for (MessageAndMetadata<String, String> aKafkaStream : kafkaStream) {
            String message = aKafkaStream.message();
            System.out.println("receive message:" + message);
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
