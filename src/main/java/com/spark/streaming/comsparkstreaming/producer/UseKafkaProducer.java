package com.spark.streaming.comsparkstreaming.producer;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * kafka 生产者
 **/
public class UseKafkaProducer {
    private KafkaProducer<String, String> producer;
    private final String topic;
    private AtomicInteger msg = new AtomicInteger(1);

    public static void main(String[] args) {
        UseKafkaProducer kafkaProducer = new UseKafkaProducer("message");
        while (true) {
            kafkaProducer.send();
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private UseKafkaProducer(String topic) {
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.20.48:9092");
        props.put("bootstrap.servers", "192.168.20.48:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

    private void send() {
        String message = "Message_" + msg.getAndIncrement() + " Time:" + DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
        producer.send(new ProducerRecord<>(topic, message));
    }
}
