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
        System.out.println("Next mod");

        SparkConf conf = new SparkConf().setAppName("Streaming Homework").setMaster("local[*]");

        //JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(3000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "stream-hw");
        kafkaParams.put("kafka.consumer.id", "kafka-consumer-01");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

/*
        //WORKING!
        OffsetRange[] offsetRanges = {
                OffsetRange.create("some", 0, 0, 100)
        };
        JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
                sparkContext,
                kafkaParams,
                offsetRanges,
                LocationStrategies.PreferConsistent()
        );
        rdd.map(ConsumerRecord::value).collect().forEach(System.out::println);
*/
        //Map<TopicPartition, Long> fromOffsets = new HashMap<>();
        //fromOffsets.put(new TopicPartition("some",0),111L);

        Collection<String> topic = Collections.singletonList("some");
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topic, kafkaParams)//, fromOffsets)
                );
        //stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        stream.foreachRDD(rdd -> {
            //rdd.map(ConsumerRecord::value).collect().forEach(System.out::println); //!

            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            System.out.print("RDD COUNT: ");
            System.out.println( rdd.rdd().count() );

//            System.out.println( "Cons record len: "+ rdd.rdd().collect().length );

            for (int i=0; i<offsetRanges.length; i++) {
                System.out.println( "Count "+offsetRanges[i].count() );
                System.out.println( "From "+offsetRanges[i].fromOffset() );
                System.out.println( "Part "+offsetRanges[i].partition() );
                System.out.println( "Topic "+offsetRanges[i].topic() );
                System.out.println( "Until "+offsetRanges[i].untilOffset() );
            }

            /*
            rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
            */
        });

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
