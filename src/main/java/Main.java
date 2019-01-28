import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

public class Main {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming Homework");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        Duration duration = new Duration(2000);
        JavaStreamingContext streamingContext = new JavaStreamingContext(javaSparkContext, duration);

        Collection<String> topic = Collections.singletonList("some");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topic, kafkaParams)
                );

        DStream<Tuple2<String, String>> dStream = stream
                .mapToPair(record -> new Tuple2<>(record.key(), record.value()))
                .dstream();

        dStream.saveAsTextFiles("hdfs://sandbox-hdp.hortonworks.com:8020/user/hadoop/spark/","txt");

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
