package com.spark.streaming.comsparkstreaming.component;

import com.mongodb.BasicDBObject;
import com.spark.streaming.comsparkstreaming.builder.KafkaStreamBuilder;
import com.spark.streaming.comsparkstreaming.config.KafkaProperties;
import com.spark.streaming.comsparkstreaming.config.MongodbProperties;
import com.spark.streaming.comsparkstreaming.config.SparkProperties;
import com.spark.streaming.comsparkstreaming.entity.OrderEvent;
import com.spark.streaming.comsparkstreaming.entity.TotalProducts;
import com.spark.streaming.comsparkstreaming.processor.KafkaProcessor;
import com.spark.streaming.comsparkstreaming.utils.MongoWriter;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @ClassName SparkStreamingRunner
 * @Description TODO
 * @Author hasee
 * @Date 2018/11/26
 * @Version 1.0
 **/
@Component
public class SparkStreamingRunner implements ApplicationRunner {

    @Autowired
    private SparkProperties sparkProperties;

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private MongodbProperties mongodbProperties;

    @Autowired
    private JavaStreamingContext streamingContext;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        MongoWriter<TotalProducts> mongoWriter = new MongoWriter<>("com.spark.streaming.comsparkstreaming.entity.TotalProducts", mongodbProperties);

        //获取输入流
        @SuppressWarnings("unchecked")
        JavaDStream<OrderEvent> javaDStream = (JavaDStream<OrderEvent>) (new KafkaStreamBuilder())
                .setContext(streamingContext)
                .setKafkaProperties(kafkaProperties)
                .setSparkProperties(sparkProperties)
                .builder();
        javaDStream.cache();

        List<Tuple2<String, Long>> tps = new ArrayList<>();
        BasicDBObject basicDBObject = new BasicDBObject();
        basicDBObject.put("recordDate", DateFormatUtils.format(new Date(), "yyyy-MM-dd"));

        List<TotalProducts> read = mongoWriter.read(basicDBObject);
        for (TotalProducts prod : read) {
            tps.add(new Tuple2<>(prod.getProductId(), prod.getTotalCount()));
        }
        JavaPairRDD<String, Long> javaPairRDD = streamingContext.sparkContext().parallelizePairs(tps);

        //实时数据处理
        KafkaProcessor processor = new KafkaProcessor();
        JavaDStream<TotalProducts> productStream = processor.process(javaDStream, javaPairRDD);

        //输出数据，保存到mongodb
        mongoWriter.save(productStream);

        //启动上下文
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
