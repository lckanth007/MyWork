package com.worldpay.fraudsightBDA
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.Row
import java.io.File
import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.sql.types._

import org.apache.spark.{SparkConf}

object FraudsightToBDA {


  def main(args: Array[String]): Unit = {

    val props: Properties = new Properties()
    val jobPropPath = new File(args(0))
    val sslPropPath = new File(args(1))
    val jobProp = new FileInputStream(jobPropPath)
    val sslProp = new FileInputStream(sslPropPath)
    props.load(jobProp)
    props.load(sslProp)

    val sparkConf = new SparkConf().setAppName(props.getProperty("AppName"))
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    sparkConf.set("spark.scheduler.mode", "FAIR")
    sparkConf.set("spark.streaming.kafka.consumer.cache.enabled", "false")
    val ssc = new StreamingContext(sparkConf, Seconds(props.getProperty("BATCH_INVL").toInt))

    val basePath = s"${props.getProperty("basePath")}"
    val extractDate = s"${props.getProperty("extractDate")}"
    val fileName = s"${props.getProperty("fileName")}"
    val topicsList = s"${props.getProperty("topics")}".split(",")
    val kafkaParams = Map(
      "ssl.client.auth" -> s"${props.getProperty("ssl_auth")}",
      "security.protocol" -> s"${props.getProperty("security.protocol")}",
      "sasl.kerberos.service.name" -> s"${props.getProperty("sasl_service_name")}",
      "ssl.truststore.location" -> s"${props.getProperty("ssl.truststore.location")}",
      "ssl.truststore.password" -> s"${props.getProperty("ssl.truststore.password")}",
      "ssl.keystore.location" -> s"${props.getProperty("ssl.keystore.location")}",
      "ssl.keystore.password" -> s"${props.getProperty("ssl.keystore.password")}",
      "bootstrap.servers" -> s"${props.getProperty("bootstrap.servers")}",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> s"${props.getProperty("group_id")}",
      //"auto.offset.reset" -> s"${props.getProperty("offset_reset")}",
      "session.timeout.ms" -> s"${props.getProperty("session_timeout_ms")}",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val schemaString = s"${props.getProperty("Schema_String")}"

    val fields = schemaString
      .split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val parquetCustomSchema = StructType(fields)

    for (topic <- topicsList) {
      val kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent, //Distribute partitions evenly across available executors
        Subscribe[String, String](Array(topic), kafkaParams)
      )
      def check(row: ((String, String), Int), value:String) = row._1._2 == value

      kafkaStream.foreachRDD { consumerRecordRdd =>
        val offsetRanges = consumerRecordRdd.asInstanceOf[HasOffsetRanges].offsetRanges
        if (!consumerRecordRdd.isEmpty()) {
          val sqlContext = SQLContext.getOrCreate(consumerRecordRdd.sparkContext)

          val chargeBckDF = sqlContext.read.json(consumerRecordRdd.map(
            x => (
              x.value().toString.filter(check(_,"chargebackId"))
              )
          )
          )
          val fraudDF = sqlContext.read.json(consumerRecordRdd.map(
            x => (
              x.value().toString.filter(check(_,"fraudTransactionId"))
              )
          )
          )
          val AsyncoutDF = sqlContext.read.json(consumerRecordRdd.map(
            x => (
              x.value().toString.filter(check(_,"fraudTransactionId"))
              )
          )
          )

          chargeBckDF.printSchema()


          //            createDataFrame(
          //              consumerRecordRdd.map(
          //                cRecord => (
          //                  cRecord.value(),
          //                  cRecord.offset(),
          //                  java.time.LocalTime.now().toString,
          //                  java.time.LocalDate.now().toString
          //                )
          //              ).map(transferedCRecord => Row(transferedCRecord._1, transferedCRecord._2, transferedCRecord._3)),
          //              parquetCustomSchema
          //            )i

//          if (topic == "eventsIn") {
            val selectFeilds = topicValueDF.select("accountNumber","cardPr")
            
            selectFeilds
            .coalesce(1)
            .write
            .mode(SaveMode.Append)
            .parquet(basePath + topic + extractDate + java.time.LocalDate.now().toString)
//        }
      }
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }


}
