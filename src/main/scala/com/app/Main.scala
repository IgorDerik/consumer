package com.app

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}

object Main extends App {

  val pathToCSV = "hdfs://sandbox-hdp.hortonworks.com:8020/user/hadoop/stream1"
  val topic = "new1"
  val duration = Duration(3000)

  val conf = new SparkConf().setAppName("Streaming Homework").setMaster("local[*]")
  val streamingContext = new StreamingContext(conf, duration)

  val sparkSession = SparkSession.builder.getOrCreate

  val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "sandbox-hdp.hortonworks.com:6667",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "stream-hw",
  "kafka.consumer.id" -> "kafka-consumer-01",
  "auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val fromOffsets = StreamUtils.getFromOffsets(topic, sparkSession, pathToCSV)
  val stream = StreamUtils.getStream(streamingContext, topic, kafkaParams, fromOffsets)
  StreamUtils.writeRDDs(stream, sparkSession, pathToCSV)

  streamingContext.start()
  streamingContext.awaitTermination()

}