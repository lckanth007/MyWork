package com.worldpay.spark


import java.net.URI
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{current_date, to_date, unix_timestamp}
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.types._;
import scala.io.Source
import scala.sys.process._
import org.apache.spark.sql.functions.udf


object bdaIngest {

  def reformat_date(time_stamp: String): String = {
    val time_stamp_lng = time_stamp.length()
    if (time_stamp_lng == 10) {
      if (time_stamp(10) == ':') {
        val b = time_stamp.substring(0, 10) + " " + time_stamp.substring(11)
        b.concat(" 00:00:00.000000")
      }
      else {
        time_stamp.concat(" 00:00:00.000000")
      }
    } else if (time_stamp_lng == 19) {
      if (time_stamp(10) == ':') {
        val b = time_stamp.substring(0, 10) + " " + time_stamp.substring(11)
        b.concat(".000000")
      }
      else {
        time_stamp.concat(".000000")
      }
    } else if (time_stamp_lng == 26) {
      if (time_stamp(10) == ':') {
        val b = time_stamp.substring(0, 10) + " " + time_stamp.substring(11)
        b
      }
      else {
        time_stamp
      }

    } else {
      val b = "00-00-00 00:00:00.000000"
      b
    }
  }



  def main(args: Array[String]) {

    val inputPath = args(0)
    var Exitcode = 0

    val spark = SparkSession
      .builder()
      .appName("bdaSparkIngestion")
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.debug.maxToStringFields", 100)
      .config("spark.scheduler.mode", "FAIR")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    try {

      val job_input = spark.sparkContext.textFile(inputPath)

      val job_properties = job_input.collect().toList.flatMap(x => x.split("=")).grouped(2).collect { case List(k, v) => k -> v }.toMap

      val TBL_NAME = job_properties("TBL_NAME")

      val TIME_COLS_ARRAY = job_properties("TIME_COLS").split(",")

      val formatting = udf(reformat_date _)

      println("Proceeding with data file")

      var srcDF = spark.read
                    .options(Map("delimiter" -> "\u0007", "ignoreLeadingWhiteSpace" -> "True", "ignoreTrailingWhiteSpace" -> "True", "multiline" -> job_properties("MVAL"), "quoteMode" -> "NONE"))
                    .csv((job_properties("SRC_FILE_PATH") + "/" + job_properties("TAR_FILE_PATH")))

      for (i <- TIME_COLS_ARRAY) {
        var tempDF = srcDF.withColumn(i, formatting(col(i)))
        srcDF = tempDF
      }

      import spark.sqlContext.implicits._
      val dataNoDupDF = srcDF.withColumn("PmtId_rownum", row_number().over(Window.partitionBy(col(job_properties("Dedup_Key"))).orderBy(col(job_properties("Dedup_order_Key")).desc))).filter($"PmtId_rownum" === 1).drop("PmtId_rownum")

      dataNoDupDF.write.options(Map("delimiter" -> "\u0007")).mode(SaveMode.Overwrite).csv((job_properties("TAR_FILE_PATH") + "/" + job_properties("TAR_FILE_NM")))

    } catch {
      case e: Throwable =>
        println("File Not Found" + e)
        Exitcode = 1
    } finally {
      spark.stop()
    }
  }
}
