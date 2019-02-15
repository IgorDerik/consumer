package com.app

import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.reflect.io.Path
import scala.util.Try

class StreamUtilsTest extends FlatSpec with BeforeAndAfter {

  private var sparkConf: SparkConf = _
  private var ssc: StreamingContext = _
  private var sparkSession: SparkSession = _

  private val pathToCSV: String = "src/test/resources/stream"
  private val topic: String = "hotels10"

  private val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "stream-hw",
    "kafka.consumer.id" -> "kafka-consumer-01",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def pathFolderExists(): Boolean = {
    Files.exists(Paths.get(pathToCSV))
  }

  before {
    val kafkaProps = new Properties()
    for ( (k,v) <- kafkaParams ) kafkaProps.put(k,v)

    try {
      AdminClient.create(kafkaProps)
          .listTopics(new ListTopicsOptions()
          .timeoutMs(3000)).listings().get()
    } catch {
      case e: Exception =>
        println("Kafka is not available: "+e.getMessage)
        println("Please install and configure it locally before testing, you can find all details in the README file.")
        sys.exit
    }

      sparkConf = new SparkConf().setMaster("local[*]").setAppName("test-streaming")
      ssc = new StreamingContext(sparkConf, Seconds(1))
      sparkSession = SparkSession.builder.getOrCreate
    }

  after {
    Thread.sleep(1000)

    ssc.stop()
    sparkSession.stop()

    Try(Path(pathToCSV).deleteRecursively)
  }

  behavior of "StreamUtils.writeRDDs method"
  it should "save records to the disk" in {

    assert(!pathFolderExists())

    val fromOffsets = StreamUtils.getFromOffsets(topic, sparkSession, pathToCSV)
    assert(fromOffsets.toList.head._2 == 0L)

    val stream = StreamUtils.getStream(ssc, topic, kafkaParams, fromOffsets)
    StreamUtils.writeRDDs(stream, sparkSession, pathToCSV)

    ssc.start()
    ssc.awaitTerminationOrTimeout(2000)

    Thread.sleep(2000)

    assert(pathFolderExists())

    val actualDF = sparkSession.read.csv(pathToCSV)
    assert(actualDF.count()==10)

    val expectedDF = sparkSession.read.csv("src/test/resources/hotels10.csv")
    assert(actualDF.collect.sameElements(expectedDF.collect))
  }

}