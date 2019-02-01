package com.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("preprod mod");
        String pathToCSV = "hdfs://sandbox-hdp.hortonworks.com:8020/user/hadoop/stream1";
        String topic = "new1";

        SparkConf conf = new SparkConf().setAppName("Streaming Homework").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(3000));

        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "stream-hw");
        kafkaParams.put("kafka.consumer.id", "kafka-consumer-01");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Configuration fsConf = new Configuration();
        fsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        fsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem.get(URI.create(pathToCSV), fsConf);

        Map<TopicPartition, Long> fromOffsets = StreamUtils.getFromOffsets(topic, sparkSession, pathToCSV);
        JavaInputDStream<ConsumerRecord<String, String>> stream = StreamUtils.getStream(streamingContext, topic, kafkaParams, fromOffsets);
        StreamUtils.writeRDDs(stream, sparkSession, pathToCSV);

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
