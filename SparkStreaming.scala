package com.worldpay.sparkStreaming

//spark2-shell --queue ingestion --master yarn --driver-memory 10G --executor-memory 20G --num-executors 10
//commit cannot be completed since the group has already rebalanced and assigned the partitions to another member.
//This means that the time between subsequent calls to poll() was longer than the configured session.timeout.ms,
//which typically implies that the poll loop is spending too much time message processing.
//You can address this either by increasing the session timeout or
//by reducing the maximum size of batches returned in poll() with max.poll.records

import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, TaskContext}
import java.time.{Instant, ZoneId, ZonedDateTime}

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

object SparkStreaming {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafkaconsumer")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    sparkConf.set("spark.streaming.concurrentJobs", "2")
    sparkConf.set("spark.scheduler.mode", "FAIR")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.set("spark.streaming.kafka.consumer.cache.enabled", "false")


    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams = Map(
      "ssl.client.auth" -> "required",
      "security.protocol" -> "SSL",
      "sasl.kerberos.service.name" -> "kafka",
      "ssl.truststore.location" -> "**",
      "ssl.truststore.password" -> "**t",
      "ssl.keystore.location" -> "***",
      "ssl.keystore.password" -> "**2",
      "bootstrap.servers" -> "10.22.33.78:9094,10.22.33.81:9094,10.22.33.87:9094",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "session.timeout.ms" -> "29999",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicsList = Array("SyncOut","AsyncOut")

    for (topic <- topicsList) {
      val kafkaStream= KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,//Distribute partitions evenly across available executors
        Subscribe[String, String](Array(topic), kafkaParams)
      )
      kafkaStream.foreachRDD { rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd.saveAsTextFile("/user/n46995a/output/featurespace12/" + topic + "/part-")
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
