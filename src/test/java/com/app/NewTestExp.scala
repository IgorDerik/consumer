package com.app

import java.nio.file.{Files, Paths}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.reflect.io.Path
import scala.util.Try

class NewTestExp extends FlatSpec with BeforeAndAfter {

  private var sc:SparkContext = _
  private var ssc: StreamingContext = _

  before {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-streaming")

    ssc = new StreamingContext(sparkConf, Seconds(1))
    sc = ssc.sparkContext
  }

  after {
    val folderExists = Files.exists(Paths.get("src/test/resources/stream"))
    println("After: " + folderExists)
    assert(folderExists)

    Try(Path("src/test/resources/stream").deleteRecursively)
  }

  behavior of "write rdd method"

  it should "write records to the path" in {

    val pathToCSV = "src/test/resources/stream"
    val topic = "hotels10"

    val sparkSession = SparkSession.builder.getOrCreate

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream-hw",
      "kafka.consumer.id" -> "kafka-consumer-01",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val fromOffsets = StreamUtils.getFromOffsets(topic, sparkSession, pathToCSV)

    val stream = StreamUtils.getStream(ssc, topic, kafkaParams, fromOffsets)

    StreamUtils.writeRDDs(stream, sparkSession, pathToCSV)

    ssc.start()

    val folderExists = Files.exists(Paths.get(pathToCSV))
    println(folderExists)

    ssc.awaitTerminationOrTimeout(3000)

  }

}