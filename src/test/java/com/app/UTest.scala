package com.app

import org.junit.Test
import org.junit.Before
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import java.util

import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.mutable

class UTest {

  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-streaming-testing-example")

  val ssc = new StreamingContext(sparkConf, Seconds(1))

  val sparkSession: SparkSession = SparkSession.builder.getOrCreate

  val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
  var consumer: MockConsumer[String, String] = _

  @Before def setUp(): Unit = {
    consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
//    consumer.
  }

  @Test
  def testConsumer(): Unit = {

    val rddQueue = mutable.Queue[RDD[ConsumerRecord[String, String]]]()

//    val s = ssc.queueStream( rddQueue )
/*
    ssc.queueStream(rddQueue)
        .window(windowDuration = Seconds(3), slideDuration = Seconds(2))
        .foreachRDD{ rdd =>
          println(rdd.count)
        }
*/
    //ssc.start()
    val prodRec0 = new ProducerRecord[String, String]("my_topic", 0, "mykey", "myvalue0")
    val prodRec1 = new ProducerRecord[String, String]("my_topic", 0, "mykey", "myvalue1")
    val prodRec2 = new ProducerRecord[String, String]("my_topic", 0, "mykey", "myvalue2")
    val prodRec3 = new ProducerRecord[String, String]("my_topic", 0, "mykey", "myvalue3")
    val prodRec4 = new ProducerRecord[String, String]("my_topic", 0, "mykey", "myvalue4")
    producer.send(prodRec0)
    producer.send(prodRec1)
    producer.send(prodRec2)
    producer.send(prodRec3)
    producer.send(prodRec4)

    consumer.assign(util.Arrays.asList(new TopicPartition("my_topic", 0)))
    val beginningOffsets = new util.HashMap[TopicPartition, java.lang.Long]()
    beginningOffsets.put(new TopicPartition("my_topic", 0), 0L)
    consumer.updateBeginningOffsets(beginningOffsets)
    /*
    consumer.addRecord(new ConsumerRecord[String, String]("my_topic", 0, 0L, "mykey", "myvalue0"))
    consumer.addRecord(new ConsumerRecord[String, String]("my_topic", 0, 1L, "mykey", "myvalue1"))
    consumer.addRecord(new ConsumerRecord[String, String]("my_topic", 0, 2L, "mykey", "myvalue2"))
    consumer.addRecord(new ConsumerRecord[String, String]("my_topic", 0, 3L, "mykey", "myvalue3"))
    consumer.addRecord(new ConsumerRecord[String, String]("my_topic", 0, 4L, "mykey", "myvalue4"))
    */
    val records: ConsumerRecords[String,String] = consumer.poll(5000)

//    StreamUtils.writeStringRDDs(,"")
    for (rec <- records.iterator) {
      //println( rec.value )
      rddQueue += ssc.sparkContext.parallelize(List(rec))
    }

    val s = ssc.queueStream( rddQueue )

    StreamUtils.writeRDDs(s, sparkSession, "src/test/resources/stream0")

    ssc.start()
    ssc.awaitTermination()

  }

}
