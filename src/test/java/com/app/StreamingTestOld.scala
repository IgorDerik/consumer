package com.app

import com.app.StreamUtils._
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ClockWrapper, Seconds, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.io.Path
import scala.util.Try

class StreamingTestOld extends FlatSpec with Matchers with Eventually with BeforeAndAfter {

  private val master = "local[*]"
  private val appName = "spark-streaming-test"
  private val filePath: String = "src/test/resources/file"

  private var ssc: StreamingContext = _
  private var sparkSession: SparkSession = _

  private val batchDuration = Seconds(1)

  var clock: ClockWrapper = _

  before {
    val conf = new SparkConf()
      .setMaster(master).setAppName(appName)
      .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

    ssc = new StreamingContext(conf, batchDuration)
    sparkSession = SparkSession.builder.getOrCreate
    clock = new ClockWrapper(ssc)
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
    //Try(Path(filePath + "-1000").deleteRecursively)
  }

  "Streaming App " should " store streams into a file" in {
    //val lines = mutable.Queue[RDD[ConsumerRecord[String, String]]]()
    val lines = mutable.Queue[RDD[String]]()
    val dstream = ssc.queueStream(lines)

    dstream.print()
//    writeStringRDDs(dstream, filePath)
    //writeRDDs(dstream, sparkSession, filePath)
    //processStream(Array("", "", filePath), dstream)

    ssc.start()

    //val rec = new ConsumerRecord[String, String]("topic", 0, 0, "k1", "v1")
    //val rec2 = new ConsumerRecord[String, String]("topic", 0, 1, "k2", "v2")
    //lines += ssc.sparkContext.parallelize(List(rec, rec2))
    //lines += ssc.sparkContext.makeRDD(Seq(rec, rec2))
    lines += ssc.sparkContext.makeRDD(Seq("b", "c"))
    clock.advance(1000)

    eventually(timeout(2 seconds)){
      val wFile: RDD[String] = ssc.sparkContext.textFile(filePath+ "-1000")
      wFile.count() should be (2)
      wFile.collect().foreach(println)
    }

  }
}
