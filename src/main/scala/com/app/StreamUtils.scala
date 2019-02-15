package com.app

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object StreamUtils {

  /**
    * Method reading input stream and write data as CSV files
    * @param stream to read from
    * @param sparkSession for creating data frames to write data as CSV
    * @param pathToCSV path to the directory will contain saved data
    */
  def writeRDDs(stream: InputDStream[ConsumerRecord[String, String]], sparkSession: SparkSession, pathToCSV: String): Unit = {

    val structType = new StructType()
      .add("offset", DataTypes.LongType)
      .add("value", DataTypes.StringType)

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty) {
        val offsetsAndValuesPairRDD: RDD[(Long, String)] = rdd.map(record => (record.offset, record.value))
        val offsetsAndValuesRowRDD = offsetsAndValuesPairRDD.map(tuple => Row(tuple._1, tuple._2))
        val offsetsAndValuesDF: DataFrame = sparkSession.createDataFrame(offsetsAndValuesRowRDD, structType)

        offsetsAndValuesDF.write.mode(SaveMode.Append).csv(pathToCSV)

        println(offsetsAndValuesDF.count + "records saved to file system" )
      }
      else println("RDD IS EMPTY")
    }

  }

  /**
    * Method for determining from which offset data should start reading
    * @param topic is kafka topic
    * @param partition of the target kafka topic
    * @param sparkSession for receiving data frame by reading directory with data (if directory exists)
    * @param pathToCSV path to the target directory should contain csv data
    * @return Map contains topic and start offset information
    */
  def getFromOffsets(topic: String, partition: Int, sparkSession: SparkSession, pathToCSV: String): Map[TopicPartition, Long] = {
    var offSet = 0L
    try {
      val fsConf = new Configuration
      fsConf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
      fsConf.set("fs.file.impl", classOf[LocalFileSystem].getName)
      FileSystem.get(URI.create(pathToCSV), fsConf)
      val rows = sparkSession.read.csv(pathToCSV)
      offSet = rows.count
    } catch {
      case e: Exception =>
        System.out.println("Probably path does not exist yet, offset will be set to 0")
    }
    Map(new TopicPartition(topic, partition) -> offSet)
  }

  /**
    * Method for determining from which offset data should start reading
    * It doesn't contain partition parameter
    * It will call previous method with default partition parameter (0)
    * @return Map contains topic and start offset information
    */
  def getFromOffsets(topic: String, sparkSession: SparkSession, pathToCSV: String): Map[TopicPartition, Long] = getFromOffsets(topic, 0, sparkSession, pathToCSV)

  /**
    * Method for getting stream from Kafka
    * @param streamingContext streaming context
    * @param topic is kafka topic to read from
    * @param kafkaParams Map contains Kafka parameters
    * @param fromOffsets offset from which data should start reading
    * @return input direct stream of consumer records from Kafka
    */
  def getStream(streamingContext: StreamingContext, topic: String, kafkaParams: Map[String, AnyRef], fromOffsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    val topicList = List(topic)
    KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicList, kafkaParams,fromOffsets))
  }

}