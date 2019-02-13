package com.app

import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.Test
import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecords, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

class MockProdTest {

  val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
  var consumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)

  @Test
  def test(): Unit = {
    val prodRec0 = new ProducerRecord[String, String]("my_topic", "mykey", "myvalue0")
    val prodRec1 = new ProducerRecord[String, String]("my_topic", "mykey", "myvalue1")
    val prodRec2 = new ProducerRecord[String, String]("my_topic", "mykey", "myvalue2")
    val prodRec3 = new ProducerRecord[String, String]("my_topic", "mykey", "myvalue3")
    val prodRec4 = new ProducerRecord[String, String]("my_topic", "mykey", "myvalue4")

    producer.send(prodRec0)
    producer.send(prodRec1)
    producer.send(prodRec2)
    producer.send(prodRec3)
    producer.send(prodRec4)

    consumer.assign(util.Arrays.asList(new TopicPartition("my_topic", 0)))
    val beginningOffsets = new util.HashMap[TopicPartition, java.lang.Long]()
    beginningOffsets.put(new TopicPartition("my_topic", 0), 0L)
    consumer.updateBeginningOffsets(beginningOffsets)

    val records: ConsumerRecords[String,String] = consumer.poll(5000)

    for (rec <- records.iterator) {
      println( rec.value )
    }
    /*
    for ( r <- producer.history.iterator ) {
      println(r.value)
    }
    */
  }

}
