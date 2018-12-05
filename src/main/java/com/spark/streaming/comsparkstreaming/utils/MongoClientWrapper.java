package com.spark.streaming.comsparkstreaming.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.spark.streaming.comsparkstreaming.config.MongodbConfig;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName MongoClientWrapper
 * @Description TODO
 * @Author hasee
 * @Date 2018/11/26
 * @Version 1.0
 **/
public class MongoClientWrapper {
    private MongoCollection<Document> collection;

    private MongoClient mongoClient;

    private MongodbConfig mongodbConfig;

    public MongoClientWrapper() {

    }

    public void setMongodbConfig(MongodbConfig mongodbConfig) {
        this.mongodbConfig = mongodbConfig;
    }

    public void connection() {
        final String connectionUrl = mongodbConfig.getUri() + "/" + mongodbConfig.getDatabase();
        MongoClientURI uri = new MongoClientURI(connectionUrl);
        mongoClient = new MongoClient(uri);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
        collection = mongoDatabase.getCollection(mongodbConfig.getCollection());
    }

    public void disConnection() {
        mongoClient.close();
    }

    public void insertOne(Document document) {
        collection.insertOne(document);
    }

    public void updateOne(Bson bson1, Bson bson2) {
        collection.updateOne(bson1, bson2, new UpdateOptions().upsert(true).bypassDocumentValidation(true));
    }

    public List<Document> find(BasicDBObject basicDBObject) {
        MongoCursor<Document> mongoCursor = collection.find(basicDBObject).iterator();
        List<Document> result = new ArrayList<>();
        while (mongoCursor.hasNext()) {
            result.add(mongoCursor.next());
        }
        return result;
    }
}
