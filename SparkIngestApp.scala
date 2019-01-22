package com.spark.ingest
import java.io.FileInputStream
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.sql.SaveMode
import java.net.URI

import java.util.Properties

import scala.io.Source.fromURL

object SparkIngestApp {
  def main(args: Array[String]) {
    //var properties : Properties = null
    //properties = new Properties()
    // for reading from resource folder
    // val reader = fromURL(getClass.getResource("job.properties")).bufferedReader()
    //  properties.load(reader);

    //for reading the properties file from the external path
    //properties.load(new FileInputStream(args(0)))

    val inputPath = args(0)
    //val inputPath = "/Users/cklekkala/IdeaProjects/SparkSample/src/main/resources/sparkJob.properties"
    var Exitcode = 0
    val StartedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
    val log = ("Started", StartedTime)
    var LogList = List(log.toString().replace("(", "").replace(")", ""))

    val spark = SparkSession
      .builder()
      .appName("SparkIngest")
      //.master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.debug.maxToStringFields",100)
      .config("spark.scheduler.mode", "FAIR")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      val processingTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
      val log1 = ("InProcessing", processingTime)


      LogList = log1.toString().replace("(", "").replace(")", "") :: LogList

      val job_input = spark.sparkContext.textFile(inputPath)
      val job_properties = job_input.collect().toList.flatMap(x => x.split("=")).grouped(2).collect { case List(k, v) => k -> v }.toMap

      val SRC_FILE_PATH = job_properties("SRC_FILE_PATH")
      val TAR_FILE_PATH = job_properties("TAR_FILE_PATH")
      val SRC_FILE_STATS_NM = job_properties("SRC_FILE_STATS_NM")
      val SRC_FILE_NM = job_properties("SRC_FILE_NM")
      val SRC_FILE_SCHEMA_NM = job_properties("SRC_FILE_SCHEMA_NM")
      val tbl_id = job_properties("tbl_id")


      //val TBL_NAME = job_properties("TBL_NAME")





      //val datapath=(properties.get("SRC_FILE_PATH")+"/"+properties.get("SRC_FILE_NM"))

     val datapath = (SRC_FILE_PATH + "/" + SRC_FILE_NM)
      val tarDatapath = (TAR_FILE_PATH+"/unzipWrite")

           val file = datapath
           val conf = new Configuration()
           val fileSystem = FileSystem.get(URI.create(file), conf)
           val compressionCodec = new CompressionCodecFactory(conf)
           val inputCodec = compressionCodec.getCodec(new Path(file))
           val outputFile = "/user/n46995a/uncompressedFile/unzipedFile.csv"
           println("Output File Name: " + outputFile)
           IOUtils.copyBytes(inputCodec.createInputStream(fileSystem.open(new Path(file))),
             fileSystem.create(new Path(outputFile)),conf)
      //val rdd = spark.sparkContext.textFile(outputFile)



      println("Source File Path :"+outputFile)
      //val schemapath=(properties.get("SRC_FILE_PATH")+"/"+properties.get("SRC_FILE_SCHEMA_NM"))

      //val schemapath = (SRC_FILE_PATH + "/" + SRC_FILE_SCHEMA_NM)

      //println(schemapath)

      println("Reading the data file in dataFrame: "+outputFile)

      val dataDF1 = spark.read
        //.schema(input6)
        .option("delimiter", "\u0007")
        .option("ignoreLeadingWhiteSpace", "True")
        .option("ignoreTrailingWhiteSpace", "True")
        //.option("multiline", "True")
        .option("inferSchema","True")
        .option("timestampFormat","yyyy-MM-dd:HH:mm:ss")
        .option("escape", "\u000D")
        .csv(outputFile)
      println("Loading the data file into dataFrame completed ")
      println("number of partion of dataFram dataDF1: "+dataDF1.rdd.partitions.size)

      //   .option("path",datapath)

      //val dftbl=properties.get("TBL_NAME") + "_df"

      //val dftbl = TBL_NAME + "_df"


      println("Data file Found")
      println("Proceeding with schema file")
      //val input = spark.sparkContext.textFile(schemapath).coalesce(1).mapPartitions(_.drop(3)).filter(line => !(line.contains(")")))

      // println("Schema file found")

      /*(val input2 = input.map { x =>
        val w = x.split(":")
        val columnName = w(0).trim()
        val raw = w(1).trim()
        (columnName, raw)
      }*/

     /* val input3 = input2.map { x =>
        val x2 = x._2.replaceAll(";", "")
        (x._1, x2)
      }

      val input4 = input3.map { x =>
        val pattern1 = ".*int\\d{0,}".r
        val pattern2 = ".*string\\[.*\\]".r
        val pattern3 = ".*timestamp\\[.*\\]".r
        val pattern4 = ".*date\\d{0,}".r
        val raw1 = pattern1 replaceAllIn (x._2, "int")
        val raw2 = pattern2 replaceAllIn (raw1, "string")
        val raw3 = pattern3 replaceAllIn (raw2, "timestamp")
        val raw4 = pattern4 replaceAllIn (raw3, "date")
        val raw5 = x._1 + " " + raw4
        raw5
      }*/

      //val tblname=properties.get("TAR_FILE_NAME")
      //val tblname = TBL_NAME

      //spark.sql("drop table if exists "+ tblname)
      //val input5 = "create table if not exists " + tblname + "(" + input4.collect().toList.mkString(",") + ") stored as parquetfile"
      //Table created in hive default database

      //spark.sql(input5)
      //dataDF1.write.insertInto(tblname.toString)
      //spark.sql("insert into table "+ tblname + " select * from " + dftbl )
      println("before write log")


      //val dataNoDupDF= dataDF1.withColumn("PmtId_rownum", row_number().over(Window.partitionBy("_c7").orderBy(col("_c28").desc))).filter(col("PmtId_rownum") === 1).drop("PmtId_rownum")

      val completedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm_ss").format(LocalDateTime.now)
      println("before write log with date"+completedTime)
      println("write DataFrame to target Location started: "+tarDatapath)

      dataDF1.write.options(Map("delimiter" -> "\u0007")).mode(SaveMode.Overwrite).csv(tarDatapath+"extract_date="+completedTime)
       // .coalesce(1)
      println("write DataFrame to target Location Completed: "+tarDatapath)
      val log2 = ("Completed", completedTime)
      LogList = log2.toString().replace("(", "").replace(")", "") :: LogList

   } catch {
      case e: Throwable =>
        println("File Not Found: "+e)
        val failedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
        Exitcode = 1
        val log3 = ("failed", failedTime)
        LogList = log3.toString().replace("(", "").replace(")", "") :: LogList
    } finally {
      //spark.sparkContext.parallelize(LogList).saveAsTextFile("/Users/cklekkala/IdeaProjects/untitled1/injecti.log")
      //   spark.sparkContext.parallelize(LogList).coalesce(1,false).saveAsTextFile(args(0)+"/SimpleApp.log")
      //System.exit(Exitcode)
      spark.stop()

    }

      //spark.stop()

  }


}
