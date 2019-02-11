package com.app;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class StreamUtilsTestOld {

    private final String topic = "topic";
    private final MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    private final List<String> testList = Arrays.asList("0,195,2,0,8803,0,3,151,1236,82",
            "1,66,3,0,12009,,1,0,9,6/26/2015",
            "2,66,2,0,12009,1,2,50,368,95",
            "3,3,2,3,66,0,6,105,35,25",
            "4,3,2,3,23,0,6,105,35,82",
            "5,3,2,3,3,0,6,105,35,8",
            "6,3,2,0,3,1,6,105,35,15",
            "7,23,2,1,23,0,3,151,1236,30",
            "8,23,2,1,8278,0,2,50,368,91",
            "9,23,2,1,8278,0,2,50,368,0");

    @Test
    public void test() throws Exception {
//        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key", "value");

        testList.forEach( line -> mockProducer.send(new ProducerRecord<>(topic, line)) );

        SparkConf conf = new SparkConf().setAppName("Streaming Homework").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(1000));
        SparkSession sparkSession = SparkSession.builder().getOrCreate();


/*
        Future<RecordMetadata> metadata = mockProducer.send(record);
        assertTrue("Send should be immediately complete", metadata.isDone());
        assertEquals("Offset should be 0", 0L, metadata.get().offset());
        assertEquals(topic, metadata.get().topic());
*/
    }
}