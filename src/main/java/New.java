import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

public class New {

    public static void main(String[] args) throws Exception {
        System.out.println("New mod");

        SparkConf conf = new SparkConf().setAppName("Streaming Homework").setMaster("local[*]");
        conf.set("spark.testing.memory", "2147480000");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(3000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "stream-hw");
        kafkaParams.put("kafka.consumer.id", "kafka-consumer-01");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Map<TopicPartition, Long> fromOffsets = new HashMap<>();
        fromOffsets.put(new TopicPartition(args[0],0),0L);

        Collection<String> topic = Collections.singletonList(args[0]);
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topic, kafkaParams, fromOffsets)
                );
        //stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
  /*
        Configuration fsConf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem.get(URI.create("hdfs://sandbox-hdp.hortonworks.com:8020/user/hadoop/"), fsConf);
*/

        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        StructType structType = new StructType()
                .add("offset", DataTypes.LongType)
                .add("value", DataTypes.StringType);

        //stream.dstream().saveAsTextFiles("hdfs://sandbox-hdp.hortonworks.com:8020/user/hadoop/", "txt");

        stream.foreachRDD(rdd -> {
            //rdd.map(ConsumerRecord::value).collect().forEach(System.out::println);
            if(!rdd.isEmpty()) {
                System.out.println("WORKING...");
                JavaPairRDD<Long, String> offsetsAndValuesPairRDD = rdd.mapToPair(record -> new Tuple2<>(record.offset(), record.value()));
                JavaRDD<Row> offsetsAndValuesRowRDD = offsetsAndValuesPairRDD.map(tuple -> RowFactory.create(tuple._1(), tuple._2()));
                Dataset<Row> offsetsAndValuesDF = sparkSession.createDataFrame(offsetsAndValuesRowRDD, structType);
                offsetsAndValuesDF.show();
                offsetsAndValuesDF.write().csv("file"+offsetsAndValuesPairRDD.count());
            }
            else {
                System.out.println("RDD IS EMPTY");
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}