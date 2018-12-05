package com.spark.streaming.comsparkstreaming.utils;

import com.mongodb.BasicDBObject;
import com.spark.streaming.comsparkstreaming.config.MongodbConfig;
import com.spark.streaming.comsparkstreaming.config.MongodbProperties;
import com.spark.streaming.comsparkstreaming.entity.OutboundData;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.bson.Document;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.util.ClassUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @ClassName MongoWriter
 * @Description TODO
 * @Author hasee
 * @Date 2018/11/26
 * @Version 1.0
 **/
public class MongoWriter<T extends OutboundData> implements Serializable {
    private MongodbConfig mongodbConfig;

    private String className;

    @Autowired
    public MongoWriter(String className, MongodbProperties mongoProperties) {
        this.className = className;
        this.mongodbConfig = new MongodbConfig(mongoProperties.getUri(), mongoProperties.getDatabase(), mongoProperties.getCollection());
    }

    public List<T> read(BasicDBObject basicDBObject) {
        List<T> dataList = new ArrayList<>();
        MongoClientWrapper wrapper = new MongoClientWrapper();
        wrapper.setMongodbConfig(mongodbConfig);
        wrapper.connection();
        List<Document> documents = wrapper.find(basicDBObject);
        Class<? extends OutboundData> type;
        OutboundData outboundData = null;
        try {
            type = (Class<? extends OutboundData>) ClassUtils.forName(className, null);
            outboundData = BeanUtils.instantiateClass(type);
        } catch (ClassNotFoundException ignored) {
        }
        for (Document document : documents) {
            Objects.requireNonNull(outboundData).setDocument(document);
            dataList.add((T) outboundData);
        }
        return dataList;
    }

    public void save(JavaDStream<T> javaDStream) {
        javaDStream.foreachRDD(rdd -> rdd.foreachPartition(records -> {
            MongoClientWrapper wrapper = new MongoClientWrapper();
            wrapper.setMongodbConfig(mongodbConfig);
            wrapper.connection();
            records.forEachRemaining(record -> wrapper.updateOne(record.getCondition(), record.getUpdateValue()));
            wrapper.disConnection();
        }));
    }

    public void setMongodbConfig(MongodbConfig mongodbConfig) {
        this.mongodbConfig = mongodbConfig;
    }
}
