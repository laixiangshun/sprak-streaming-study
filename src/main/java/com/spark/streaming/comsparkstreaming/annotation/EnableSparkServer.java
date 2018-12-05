package com.spark.streaming.comsparkstreaming.annotation;

import com.spark.streaming.comsparkstreaming.config.SparkServerMarkerConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @ClassName EnableSparkServer
 * @Description TODO
 * @Author hasee
 * @Date 2018/11/26
 * @Version 1.0
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@SpringBootApplication
@ComponentScan("com.spark.streaming")
@EnableAutoConfiguration(exclude = {GsonAutoConfiguration.class,CassandraAutoConfiguration.class})
@Import(SparkServerMarkerConfiguration.class)
public @interface EnableSparkServer {

}
