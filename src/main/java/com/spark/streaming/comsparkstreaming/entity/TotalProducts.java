package com.spark.streaming.comsparkstreaming.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.commons.lang.time.DateFormatUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;


public class TotalProducts extends OutboundData {

    private String productId;

    private long totalCount;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "MST")
    private Date timeStamp;

    private String recordDate;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getRecordDate() {
        return recordDate;
    }

    public void setRecordDate(String recordDate) {
        this.recordDate = recordDate;
    }


    public void setDocument(Document doc) {
        setProductId(doc.get("productId").toString());
        setTotalCount(Long.parseLong(doc.get("totalCount").toString()));
        setRecordDate(doc.get("recordDate").toString());
        setTimeStamp(new Date(Date.parse(doc.get("timeStamp").toString())));
    }

    public Bson getCondition() {
        return (and(eq("productId", getProductId()),
                eq("recordDate", getRecordDate())
        )
        );
    }

    public Bson getUpdateValue() {
        return (combine(
                set("totalCount", getTotalCount()),
                set("timeStamp", getTimeStamp())
        )
        );
    }
}