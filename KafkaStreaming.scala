spark2-shell --queue general --master yarn --jars /opt/cloudera/parcels/SPARK2/lib/spark2/kafka-0.10//spark-streaming-kafka-0-10_2.11-2.2.0.cloudera2.jar --conf spark.driver.allowMultipleContexts=true


import Utils.Utils
import kafka.serializer._
import org.apache.spark.sql.SQLContext
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.Durations._
import kafka.utils._
import kafka.utils.ZkUtils

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaconsumer")
sparkConf.set("spark.streaming.backpressure.enabled", "true")
sparkConf.set("spark.driver.allowMultipleContexts","true")
sparkConf.set("spark.streaming.concurrentJobs", "4");

val ssc = new StreamingContext(sparkConf, Seconds(60))

val zkClientAndConnection = ZkUtils.createZkClientAndConnection("ZKIP", 60, 60)
val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)


def readOffsets(topics: Seq[String], groupId:String):
 Map[TopicPartition, Long] = {
 
 val topicPartOffsetMap = collection.mutable.HashMap.empty[TopicPartition, Long]
 val partitionMap = zkUtils.getPartitionsForTopics(topics)
 
 // /consumers/<groupId>/offsets/<topic>/
 partitionMap.foreach(topicPartitions => {
   val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
   topicPartitions._2.foreach(partition => {
     val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + partition
 
     try {
       val offsetStatTuple = zkUtils.readData(offsetPath)
       if (offsetStatTuple != null) {
         //LOGGER.info("retrieving offset details - topic: {}, partition: {}, offset: {}, node path: {}", Seq[AnyRef](topicPartitions._1, partition.toString, offsetStatTuple._1, offsetPath): _*)
 
         topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)),
           offsetStatTuple._1.toLong)
       }
 
     } catch {
       case e: Exception =>
         //LOGGER.warn("retrieving offset details - no previous node exists:" + " {}, topic: {}, partition: {}, node path: {}", Seq[AnyRef](e.getMessage, topicPartitions._1, partition.toString, offsetPath): _*)
 
         topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), 0L)
     }
   })
 })
 
 topicPartOffsetMap.toMap
}
 val fromOffsetMap:Map[TopicPartition, Long] =readOffsets(Seq("EventsIn"),"kafkasite1")

val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "IP",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafkasite1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
	
/*	val kafkaParams = Map(
       "metadata.broker.list" -> "IP",
       "group.id" -> "kafkasite1") */
	   
val kafkaStream1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,Set("EventsIn"),fromOffsetMap).map(_._2)
	val inputDStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, ConsumerStrategies.Subscribe[String,String](topics, kafkaParams, fromOffsets))
	
	
val inputDStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String,String](Set("EventsIn"), kafkaParams, fromOffsetMap)).map(_._2)

val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Set("EventsIn"), kafkaParams,fromOffsetMap)
    )

 
val kafkaStream1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,Set("EventsIn"),fromOffsetMap).map(_._2)

val inputDStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, ConsumerStrategies.Subscribe[String,String](Set("EventsIn"), kafkaParams, fromOffsetMap)).map(_._2)


val kafkaStream1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,Set("EventsIn"),fromOffsets).map(_._2)


val kafkaStream2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,Set("monitortest")).map(_._2)
val windowedStream1 = kafkaStream1.window(minutes(2))
val windowedStream2 = kafkaStream2.window(minutes(2))

windowedStream1.saveAsTextFiles("/user/Chan/output/EventsIn/EventsIn.txt")
windowedStream2.saveAsTextFiles("/user/Chan/output/monitortest/monitortest.txt")

kafkaStream1.print()
kafkaStream2.print()
ssc.start()
ssc.awaitTermination()
