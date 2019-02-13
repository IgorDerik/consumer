package com.app

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Clock, Seconds, StreamingContext}
import org.apache.spark.{FixedClock, SparkConf, SparkContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Assertion, BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration

class StreamingTest extends FlatSpec with Matchers with BeforeAndAfter with Eventually {

  var sc:SparkContext = _
  var ssc: StreamingContext = _
  var fixedClock: FixedClock = _

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
    ssc.stop(stopSparkContext = true, stopGracefully = false)
  }

  behavior of "stream transformation"

  it should "apply transformation" in {
    //val inputData: mutable.Queue[RDD[Char]] = mutable.Queue()
    val inputData: mutable.Queue[RDD[ConsumerRecord[String, String]]] = mutable.Queue()
  //  var outputCollector = ListBuffer.empty[Array[ConsumerRecord[String, String]]]

    val inputStream = ssc.queueStream(inputData)

    inputStream.foreachRDD { rdd=>
      println(rdd.count)
    }
//    val outputStream = StreamOperations.capitalizeWindowed(inputStream)

//    outputStream.foreachRDD(rdd=> {outputCollector += rdd.collect()})

    ssc.start()

    //new ConsumerRecord[String, String]("my_topic", 0, 0L, "mykey", "myvalue0")
   inputData += sc.makeRDD(List(new ConsumerRecord[String, String]("my_topic", 0, 0L, "mykey", "myvalue0")))
    wait1sec() // T = 1s

    inputData += sc.makeRDD(List(new ConsumerRecord[String, String]("my_topic", 0, 1L, "mykey", "myvalue1")))
    wait1sec() // T = 2s

    //assertOutput(outputCollector, List('A','B'))

    inputData += sc.makeRDD(List(new ConsumerRecord[String, String]("my_topic", 0, 2L, "mykey", "myvalue2")))
    wait1sec() // T = 3s

    inputData += sc.makeRDD(List(new ConsumerRecord[String, String]("my_topic", 0, 3L, "mykey", "myvalue3")))
    wait1sec() // T = 4s
    //assertOutput(outputCollector, List('B', 'C', 'D'))

    // wait until next slide
    wait1sec() // T = 5s
    wait1sec() // T = 6s
    //assertOutput(outputCollector, List('D'))
  }

  /*
  def assertOutput(result: Iterable[Array[Char]], expected: List[Char]): Assertion =
    eventually {
      result.last.toSet should equal(expected.toSet)
    }
*/
  def wait1sec(): Unit = {
    fixedClock.addTime(Seconds(1))
  }

}