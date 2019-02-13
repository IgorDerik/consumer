package com.app

import java.nio.file.{Paths, Files}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Clock, Duration, Seconds, StreamingContext}
import org.apache.spark.{FixedClock, SparkConf, SparkContext}
import org.junit.{After, Before, Test}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Assertion, BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable

class NewTest extends FlatSpec with Matchers with BeforeAndAfter with Eventually {

  var fixedClock: FixedClock = _
  var sc:SparkContext = _
  var ssc: StreamingContext = _

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(1500, Millis)))

  before {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-streaming")
      .set("spark.streaming.clock", "org.apache.spark.FixedClock")

    ssc = new StreamingContext(sparkConf, Seconds(1))
    sc = ssc.sparkContext
    fixedClock = Clock.getFixedClock(ssc)
  }

  after {
    val folderExists = Files.exists(Paths.get("src/test/resources/stream"))
    println("After: " + folderExists)
    assert(folderExists)
//    ssc.stop(stopSparkContext = true, stopGracefully = false)
  }
//  @Test
  behavior of "stream transformation"

  it should "apply transformation" in {
//  @Test
  //def test() {

    val pathToCSV = "src/test/resources/stream"
    val topic = "hotels10"
//    val duration = Duration(3000)
/*
    val conf = new SparkConf()
      .setAppName("Streaming Homework Test")
      .setMaster("local[*]")
      .set("spark.streaming.clock", "org.apache.spark.FixedClock")
*/
//    val streamingContext = new StreamingContext(conf, Seconds(1))

//    fixedClock = Clock.getFixedClock(streamingContext)

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

    waitSomeSec(1)

    //eventually {
      val folderExists = Files.exists(Paths.get(pathToCSV))
      //assert(folderExists)
    //}

    ssc.awaitTerminationOrTimeout(3000)

    //assert(folderExists)

  }

  def waitSomeSec(seconds: Long): Unit = {
    fixedClock.addTime(Seconds(seconds))
  }

}