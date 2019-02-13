package com.app

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
//import org.junit.Test

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ClockWrapper, Seconds, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.io.Path
import scala.util.Try

class StreamUtilsTest extends FlatSpec with Matchers with Eventually with BeforeAndAfter {

  private val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "stream-hw",
    "kafka.consumer.id" -> "kafka-consumer-01",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  private val pathToCSV = "src/test/resources/stream"
  private val topic = "hotels10"
//  val duration = Duration(1000)
  private var sparkSession: SparkSession = _

  private var fromOffsets: Map[TopicPartition, Long] = _
  private var stream: InputDStream[ConsumerRecord[String, String]] = _

  private var ssc: StreamingContext = _
  private val batchDuration = Seconds(1)
  var clock: ClockWrapper = _

  before {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("Streaming Homework Test")
      .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

    sparkSession = SparkSession.builder.getOrCreate

    ssc = new StreamingContext(conf, batchDuration)
    clock = new ClockWrapper(ssc)

    fromOffsets = StreamUtils.getFromOffsets(topic, sparkSession, pathToCSV)

    stream = StreamUtils.getStream(ssc, topic, kafkaParams, fromOffsets)
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
   // Try(Path(filePath + "-1000").deleteRecursively)
  }

  "Streaming App " should " store streams into a file" in {
    //val lines = mutable.Queue[RDD[String]]()
    //val dstream = ssc.queueStream(lines)
    //dstream.print()
    //processStream(Array("", "", filePath), dstream)

    ssc.start()

    //lines += ssc.sparkContext.makeRDD(Seq("b", "c"))
    clock.advance(1000)

    eventually(timeout(2 seconds)){
      val wFile: RDD[String] = ssc.sparkContext.textFile(pathToCSV)
      wFile.count() should be (200)
      wFile.collect().foreach(println)
    }

    ssc.awaitTerminationOrTimeout(5000)
  }

/*
  @Test
  def test(): Unit = {

    //val conf = new SparkConf().setAppName("Streaming Homework Test").setMaster("local[*]")
    //val streamingContext = new StreamingContext(conf, duration)

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


    //val stream = StreamUtils.getStream(streamingContext, topic, kafkaParams, fromOffsets)
    //StreamUtils.writeRDDs(stream, sparkSession, pathToCSV)

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(3000)

  }
*/
}
