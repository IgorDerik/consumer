package com.app;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StreamUtilsTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static String pathToCSV;
    private static String topic;
    private static Duration duration;
    private static SparkConf conf;
    private static JavaStreamingContext streamingContext;
    private static SparkSession sparkSession;
    private static Map<String, Object> kafkaParams;

    @Before
    public void initialize() {
        pathToCSV = tempFolder.getRoot().getPath();
        topic = "hotels10";
        duration = new Duration(1000);
        conf = new SparkConf().setAppName("Streaming Homework Testing").setMaster("local[*]");
        streamingContext = new JavaStreamingContext(conf, duration);
        sparkSession = SparkSession.builder().getOrCreate();
        kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "stream-hw");
        kafkaParams.put("kafka.consumer.id", "kafka-consumer-01");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
    }

    @After
    public void after() throws IOException {
        System.out.println(pathToCSV);
        //Files.newDirectoryStream(Paths.get(pathToCSV), p -> p.toString().endsWith(".csv")).forEach(System.out::println);
    }

    @Test
    public void test() throws Exception {
//        Map<TopicPartition, Long> fromOffsets = new HashMap<>();
  //      fromOffsets.put(new TopicPartition(topic,0), 0L);
        Map<TopicPartition, Long> fromOffsets = StreamUtilsOld.getFromOffsets(topic, sparkSession, pathToCSV);
        JavaInputDStream<ConsumerRecord<String, String>> stream = StreamUtilsOld.getStream(streamingContext, topic, kafkaParams, fromOffsets);
        StreamUtilsOld.writeRDDs(stream, sparkSession, pathToCSV);

        streamingContext.start();
//        streamingContext.awaitTermination();
        streamingContext.awaitTerminationOrTimeout(2000);

        //wait(5000);
//        System.out.println(streamingContext.getState());
            //streamingContext.getState()==StreamingContextState.STOPPED

//        Files.newDirectoryStream(Paths.get(pathToCSV), p -> p.toString().endsWith(".csv")).forEach(System.out::println);
    }

}
