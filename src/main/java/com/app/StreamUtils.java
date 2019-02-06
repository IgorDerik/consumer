package com.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StreamUtils {

    public static JavaInputDStream<ConsumerRecord<String, String>> getStream(JavaStreamingContext streamingContext, String topic, Map<String, Object> kafkaParams, Map<TopicPartition, Long> fromOffsets) {

        Collection<String> topicList = Collections.singletonList(topic);

        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topicList, kafkaParams, fromOffsets)
        );

    }

    public static void writeRDDs(JavaInputDStream<ConsumerRecord<String, String>> stream, SparkSession sparkSession, String pathToCSV) {

        StructType structType = new StructType()
                .add("offset", DataTypes.LongType)
                .add("value", DataTypes.StringType);

        stream.foreachRDD(rdd -> {

            if(!rdd.isEmpty()) {
                JavaPairRDD<Long, String> offsetsAndValuesPairRDD = rdd.mapToPair(record -> new Tuple2<>(record.offset(), record.value()));
                JavaRDD<Row> offsetsAndValuesRowRDD = offsetsAndValuesPairRDD.map(tuple -> RowFactory.create(tuple._1(), tuple._2()));
                Dataset<Row> offsetsAndValuesDF = sparkSession.createDataFrame(offsetsAndValuesRowRDD, structType);

                offsetsAndValuesDF.show();

                offsetsAndValuesDF.write().mode(SaveMode.Append)
                        .csv(pathToCSV);
            }
            else {
                System.out.println("RDD IS EMPTY");
            }
        });

    }

    public static Map<TopicPartition, Long> getFromOffsets(String topic, int partition, SparkSession sparkSession, String pathToCSV) {

        Map<TopicPartition, Long> result = new HashMap<>();

        long offSet = 0L;

        try {
            Configuration fsConf = new Configuration();
            fsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            fsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            FileSystem.get(URI.create(pathToCSV), fsConf);

            Dataset<Row> rows = sparkSession.read().csv(pathToCSV);
            offSet = rows.count();

        } catch (Exception e) {
            System.out.println("Probably path does not exist yet, offset will be set to 0");
        }

        result.put(new TopicPartition(topic,partition), offSet);
        return result;
    }

    public static Map<TopicPartition, Long> getFromOffsets(String topic, SparkSession sparkSession, String pathToCSV) {

        return getFromOffsets(topic, 0, sparkSession, pathToCSV);

    }

}