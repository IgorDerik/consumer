package com.app

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object MainStreaming {

  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-streaming-testing-example")

  val ssc = new StreamingContext(sparkConf, Seconds(1))

  def main(args: Array[String]) {

    val rddQueue = new mutable.Queue[RDD[Char]]()
    //val rddQueue2 = mutable.Queue[RDD[ConsumerRecord[String, String]]]()

    ssc.queueStream(rddQueue)
//      .map(_.toUpper)
      .window(windowDuration = Seconds(3), slideDuration = Seconds(2))
      .print()

    ssc.start()

//    val rec = new ConsumerRecord[String, String]("topic", 0, 0, "k1", "v1")
  //  val rec2 = new ConsumerRecord[String, String]("topic", 0, 1, "k2", "v2")
//    rddQueue2 += ssc.sparkContext.parallelize(List(rec,rec2))

    for (c <- 'a' to 'z') {
      rddQueue += ssc.sparkContext.parallelize(List(c))
    }

    ssc.awaitTermination()
  }

}
