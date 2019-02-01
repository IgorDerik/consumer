package com.app;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class StreamUtils {

    public static Map<TopicPartition, Long> fromOffsets(SparkSession sparkSession, String pathToCSV) {

        Map<TopicPartition, Long> result = new HashMap<>();

        long offSet = 0L;

        Path path = new Path("");

        Dataset<Row> rows = sparkSession.read().csv(pathToCSV);

        return result;

    }

}
