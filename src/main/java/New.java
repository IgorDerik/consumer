import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
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

        stream.foreachRDD(rdd -> {
            rdd.map(ConsumerRecord::value).collect().forEach(System.out::println);
        });

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}