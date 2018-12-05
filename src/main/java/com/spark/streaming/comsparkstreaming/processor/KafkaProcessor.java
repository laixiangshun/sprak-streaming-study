package com.spark.streaming.comsparkstreaming.processor;

import com.spark.streaming.comsparkstreaming.entity.OrderEvent;
import com.spark.streaming.comsparkstreaming.entity.TotalProducts;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.Date;

/**
 * @ClassName KafkaProcessor
 * @Description TODO
 * @Author hasee
 * @Date 2018/11/26
 * @Version 1.0
 **/
public class KafkaProcessor {

    public JavaDStream<TotalProducts> process(JavaDStream<OrderEvent> orderEventJavaDStream) {
        return (process(orderEventJavaDStream, null));
    }

    public JavaDStream<TotalProducts> process(JavaDStream<OrderEvent> orderDataStream, JavaPairRDD<String, Long> initState) {
        JavaPairDStream<String, Long> countDStreamPair = orderDataStream.mapToPair(
                (PairFunction<OrderEvent, String, Long>) orderEvent ->
                        new Tuple2<>(orderEvent.getProductId(), (long) orderEvent.getProdNum())
        ).reduceByKey((Function2<Long, Long, Long>)
                (aLong, aLong2) -> aLong + aLong2);
        JavaMapWithStateDStream<String, Long, Long, Tuple2<String, Long>> javaMapWithStateDStream;
        if (initState != null) {
            javaMapWithStateDStream = countDStreamPair.mapWithState(StateSpec.function((Function3<String, Optional<Long>, State<Long>, Tuple2<String, Long>>) (key, currentNum, state) -> {
                long totalNum = currentNum.or(0L) + (state.exists() ? state.get() : 0);
                Tuple2<String, Long> tuple2 = new Tuple2<>(key, totalNum);
                state.update(totalNum);
                return tuple2;
            }).timeout(Durations.seconds(60 * 60 * 24)).initialState(initState));
        } else {
            javaMapWithStateDStream = countDStreamPair.mapWithState(StateSpec.function((Function3<String, Optional<Long>, State<Long>, Tuple2<String, Long>>) (key, currentNum, state) -> {
                long totalNum = currentNum.or(0L) + (state.exists() ? state.get() : 0);
                Tuple2<String, Long> tuple2 = new Tuple2<>(key, totalNum);
                state.update(totalNum);
                return tuple2;
            }).timeout(Durations.seconds(24 * 60 * 60)));
        }

        JavaDStream<Tuple2<String, Long>> countDStream = javaMapWithStateDStream.map(tuple2 -> tuple2);
        JavaDStream<TotalProducts> productStream = countDStream.map(tuple -> {
            TotalProducts totalProducts = new TotalProducts();
            totalProducts.setProductId(tuple._1());
            totalProducts.setTotalCount(tuple._2());
            totalProducts.setTimeStamp(new Date());
            totalProducts.setRecordDate(DateFormatUtils.format(new Date(), "yyyy-MM-dd"));
            return totalProducts;
        });
        return productStream;
    }
}
