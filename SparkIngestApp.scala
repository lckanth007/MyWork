package com.spark.ingest
import java.io.FileInputStream
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import scala.io.Source.fromURL

object SparkIngestApp {
  def main(args: Array[String]) {
    var properties : Properties = null
    properties = new Properties()
    // for reading from resource folder
    // val reader = fromURL(getClass.getResource("job.properties")).bufferedReader()
    //  properties.load(reader);

    //for reading the properties file from the external path
    properties.load(new FileInputStream(args(0)))

    var Exitcode = 0
    val StartedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
    val log = ("Started", StartedTime)
    var LogList = List(log.toString().replace("(", "").replace(")", ""))

    val spark = SparkSession
      .builder()
      .appName("SparkIngest")
      // .master("local[*]")
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
      //val datapath=("file://"+properties.get("SRC_FILE_PATH")+"/"+properties.get("SRC_FILE_NM"))
      val datapath=(properties.get("SRC_FILE_PATH")+"/"+properties.get("SRC_FILE_NM"))
      println(datapath)
      // val schemapath=("file://"+properties.get("SRC_FILE_PATH")+"/"+properties.get("SRC_FILE_SCHEMA_NM"))
      val schemapath=(properties.get("SRC_FILE_PATH")+"/"+properties.get("SRC_FILE_SCHEMA_NM"))
      println(schemapath)

      println("Proceeding with data file")

      val dataDF1 = spark.read
        //.schema(input6)
        .option("delimiter", "\u0007")
        .option("ignoreLeadingWhiteSpace", "True")
        .option("ignoreTrailingWhiteSpace", "True")
        .option("multiline", "True")
        .option("escape", "\u000D")
        .csv(datapath)
      //   .option("path",datapath)

      val dftbl=properties.get("TBL_NAME") + "_df"



      println("Data file Found")
      println("Proceeding with schema file")
      val input = spark.sparkContext.textFile(schemapath).coalesce(1).mapPartitions(_.drop(3)).filter(line => !(line.contains(")")))

      // println("Schema file found")

      val input2 = input.map { x =>
        val w = x.split(":")
        val columnName = w(0).trim()
        val raw = w(1).trim()
        (columnName, raw)
      }

      val input3 = input2.map { x =>
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
      }

      val tblname=properties.get("TAR_FILE_NAME")
      //spark.sql("drop table if exists "+ tblname)
      //val input5 = "create table if not exists " + tblname + "(" + input4.collect().toList.mkString(",") + ") stored as parquetfile"
      //Table created in hive default database

      //spark.sql(input5)
      //dataDF1.write.insertInto(tblname.toString)
      //spark.sql("insert into table "+ tblname + " select * from " + dftbl )
      println("before write log")

      val completedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm_ss_n").format(LocalDateTime.now)
      println("before write log with date"+completedTime)
      dataDF1.coalesce(1).write.options(Map("delimiter" -> "\u0007")).mode(SaveMode.Overwrite).parquet("/user/n46995a/output/extract_date="+completedTime)


      val log2 = ("Completed", completedTime)
      LogList = log2.toString().replace("(", "").replace(")", "") :: LogList

    } catch {
      case e: Throwable =>
        println("File Not Found"+e)
        val failedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
        Exitcode = 1
        val log3 = ("failed", failedTime)
        LogList = log3.toString().replace("(", "").replace(")", "") :: LogList
    } finally {
      //spark.sparkContext.parallelize(LogList).saveAsTextFile("/Users/cklekkala/IdeaProjects/untitled1/injecti.log")
      //   spark.sparkContext.parallelize(LogList).coalesce(1,false).saveAsTextFile(args(0)+"/SimpleApp.log")
      System.exit(Exitcode)


    }
    spark.stop()
  }
}
