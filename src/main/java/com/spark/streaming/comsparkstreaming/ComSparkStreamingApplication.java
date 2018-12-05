package com.spark.streaming.comsparkstreaming;

import com.spark.streaming.comsparkstreaming.annotation.EnableSparkServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableSparkServer
@ComponentScan("com.spark.streaming")
public class ComSparkStreamingApplication {

    public static void main(String[] args) {
        SpringApplication.run(ComSparkStreamingApplication.class, args);
    }
}
