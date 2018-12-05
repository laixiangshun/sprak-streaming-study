package com.spark.streaming.comsparkstreaming.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * kafka 消费者 kafka-client方式
 **/
public class NewUseKafkaConsumer {
    private KafkaConsumer<String, String> consumer;
    private String topic;

    public static void main(String[] args) {
        NewUseKafkaConsumer kafkaConsumer = new NewUseKafkaConsumer("message");
        kafkaConsumer.receive();
    }

    private NewUseKafkaConsumer(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.20.48:9092");
        props.put("zookeeper.connect", "192.168.20.48:2181,192.168.20.51:2181,192.168.20.52:2181");
        props.put("group.id", "group1");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    private void receive() {
        Collection<String> topics = Collections.singletonList(topic);
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(3));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("offset:" + consumerRecord.offset());
                System.out.println("partition:" + consumerRecord.partition());
                System.out.println("消费内容：" + consumerRecord.value());
            }
        }
    }
}
