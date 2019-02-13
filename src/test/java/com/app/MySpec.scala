package com.app

import java.util.Properties

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.WordSpec

class MySpec extends WordSpec with EmbeddedKafka {

  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-streaming-testing-example")

  val ssc = new StreamingContext(sparkConf, Seconds(1))

  val sparkSession: SparkSession = SparkSession.builder.getOrCreate

  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "stream-hw",
    "kafka.consumer.id" -> "kafka-consumer-01",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  "runs with embedded kafka" should {

    "work" in {

      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2182)

      val properties = new Properties()
      properties.setProperty("bootstrap.servers", "localhost:9092")
      properties.setProperty("zookeeper.connect", "localhost:2182")
      properties.setProperty("group.id", "test")
      properties.setProperty("auto.offset.reset", "earliest")

      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        createCustomTopic("topic")
        // ... code goes here
        //publishStringMessageToKafka("topic", "mess")
        publishStringMessageToKafka("topic", "mess")


                val topicList = List("topic")
                val stream = KafkaUtils.createDirectStream[String, String](
                  ssc, PreferConsistent, Subscribe[String, String](topicList, kafkaParams)
                )

                StreamUtils.writeRDDs(stream, sparkSession, "src/test/resources/test")

        ssc.start()
        ssc.awaitTerminationOrTimeout(3000)
        //println( consumeFirstStringMessageFrom("topic") )
      }
    }
  }

}
