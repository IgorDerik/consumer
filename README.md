# Spark Job Application that reads Kafka topic and put data into HDFS

## Getting Started

Please be sure to configure your kafka properties in Main class.
After that, the recommend way of use is to build jar using maven plugin and run it with parameters using java command.

* Build project with maven plugin:
```
mvn package
```
* Run jar with 3 parameters using java:
```
java -jar pathToJarFile.jar pathToCSV topic duration

pathToJarFile.jar: Path to the generated jar file
pathToCSV: path to the hdfs directory that will contain data received from Kafka and transformed it to CSV
topic: kafka topic name that should be read
duration: batch duration of Spark Streaming
```

## StreamUtils methods

* def writeRDDs(stream: InputDStream[ConsumerRecord[String, String]], sparkSession: SparkSession, pathToCSV: String)

  * Method reading input stream and write data as CSV files
    * @param stream to read from
    * @param sparkSession for creating data frames to write data as CSV
    * @param pathToCSV path to the directory will contain saved data

* def getFromOffsets(topic: String, partition: Int, sparkSession: SparkSession, pathToCSV: String): Map[TopicPartition, Long]

  * Method for determining from which offset data should start reading
    * @param topic is kafka topic
    * @param partition of the target kafka topic
    * @param sparkSession for receiving data frame by reading directory with data (if directory exists)
    * @param pathToCSV path to the target directory should contain csv data
    * @return Map contains topic and start offset information

* def getFromOffsets(topic: String, sparkSession: SparkSession, pathToCSV: String): Map[TopicPartition, Long]

  * Method for determining from which offset data should start reading
    * It doesn't contain partition parameter
    * It will call previous method with default partition parameter (0)
    * @return Map contains topic and start offset information
    
* def getStream(streamingContext: StreamingContext, topic: String, kafkaParams: Map[String, AnyRef], fromOffsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]]

  * Method for getting stream from Kafka
    * @param streamingContext streaming context
    * @param topic is kafka topic to read from
    * @param kafkaParams Map contains Kafka parameters
    * @param fromOffsets offset from which data should start reading
    * @return input direct stream of consumer records from Kafka

## Running the tests

Please install Kafka on your local machine to be able to test the application.
By default, Kafka's bootstrap server should be running on <localhost:9092>, but you can change it by editing the class with tests.
If you want to test on Windows OS, here is an article which describe how to do this: <https://dzone.com/articles/running-apache-kafka-on-windows-os>
Also, please create a topic with name: `hotels10` and send the following text messages to it:
```
0,195,2,0,8803,0,3,151,1236,82
1,66,3,0,12009,,1,0,9,6/26/2015
2,66,2,0,12009,1,2,50,368,95
3,3,2,3,66,0,6,105,35,25
4,3,2,3,23,0,6,105,35,82
5,3,2,3,3,0,6,105,35,8
6,3,2,0,3,1,6,105,35,15
7,23,2,1,23,0,3,151,1236,30
8,23,2,1,8278,0,2,50,368,91
9,23,2,1,8278,0,2,50,368,0
```
How to create a topic and send messages to it you can read in the same article above.
After that you can run the following class to test the application behaviour:
```
src/test/scala/com/app/StreamUtilsTest.scala
```

##### Enjoy!