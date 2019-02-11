package com.app;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.junit.Test;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class StreamUtilsTestNew {

    @Test
    public void test() throws Exception {

        SparkConf conf = new SparkConf().setAppName("Streaming Homework");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(1000));

        Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();

//        streamingContext.queueStream()

        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }

        for (int i = 0; i < 30; i++) {
            rddQueue.add(streamingContext.sparkContext().parallelize(list));
        }

        streamingContext.queueStream(rddQueue).print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}