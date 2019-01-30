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
  def main(args: Array[String]) {

    val inputPath = args(0)
    var Exitcode = 0
    val StartedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
    val log = ("Started", StartedTime)
    var LogList = List(log.toString().replace("(", "").replace(")", ""))

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
      val processingTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
      val log1 = ("InProcessing", processingTime)
      println(inputPath)

      LogList = log1.toString().replace("(", "").replace(")", "") :: LogList
      println(inputPath)
      val job_input = spark.sparkContext.textFile(inputPath)

      val job_properties = job_input.collect().toList.flatMap(x => x.split("=")).grouped(2).collect { case List(k, v) => k -> v }.toMap

      val SRC_FILE_PATH = job_properties("SRC_FILE_PATH")
      val SRC_FILE_STATS_NM = job_properties("SRC_FILE_STATS_NM")
      val SRC_FILE_NM = job_properties("SRC_FILE_NM")
      val SRC_FILE_SCHEMA_NM = job_properties("SRC_FILE_SCHEMA_NM")
      val tbl_id = job_properties("tbl_id")
      val TBL_NAME = job_properties("TBL_NAME")
      val Dedup_Key = job_properties("Dedup_Key")
      val TIME_COLS = job_properties("TIME_COLS")
      val Dedup_order_Key = job_properties("Dedup_order_Key")
      val datapath = (SRC_FILE_PATH + "/" + SRC_FILE_NM)


      val a = TIME_COLS.split(",")



      def reformat_date(date: String):String = {
        val a = date.length()
        val b =19
        if(a>10){
        if (a > b){

          if (date(10) == ':'){
            val a = date.split(":")
            val b = a(0)+ " " +a(2)+":"+a(3)
            b
          }
          else{
            date
          }
        }
        else{
          if (date(10) == ':'){
            val a = date.split(":")
            val b = a(0)+ " " +a(2)+":"+a(3)
            b.concat(".000000")
          }
          else{
            date.concat(".000000")
          }
        }}
        else{
          date.concat(" 00:00:00.000000")
        }

      }

      val formatting = udf(reformat_date _)
//      val file = "datapath"
//      val conf = new Configuration()
//
//      val fileSystem = FileSystem.get(URI.create(file), conf)
//      val compressionCodec = new CompressionCodecFactory(conf)
//      val inputCodec = compressionCodec.getCodec(new Path(file))
//
//      val outputFile = "/user/n46995a/uncompressedFile/sourceFile"
//      println("Output File Name: " + outputFile)
//      IOUtils.copyBytes(
//        inputCodec.createInputStream(fileSystem.open(new Path(file))),
//        fileSystem.create(new Path(outputFile)),
//        conf)


//      println(outputFile)


      println("Proceeding with data file")

      var dataDF1 = spark.read
        .option("delimiter", "\u0007")
        .option("ignoreLeadingWhiteSpace", "True")
        .option("ignoreTrailingWhiteSpace", "True")
        .option("escape", "\u000D")
        .csv(datapath)
      import spark.sqlContext.implicits._

//      var dateFmtDF=dataDF1.withColumn("_c4", date_format(to_timestamp($"_c4", "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")).withColumn("_c28", date_format(to_timestamp($"_c28", "yyyy-MM-dd:HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")).withColumn("_c32", date_format(to_timestamp($"_c32", "yyyy-MM-dd:HH:mm:ss"), "yyyy-MM-dd HH:mm:ss"))


      for ( i <- a )
      {
        print("\nstart"+i)
        print("\ncount of df at"+i+"is:"+dataDF1.count())
        var dataDFq=dataDF1.withColumn(i, formatting(col(i)))

        print("\ncount of df at"+i+"is:"+dataDFq.count())
        dataDFq.select("_c4","_c28","_c32").show()
        dataDF1=dataDFq
        print("\nend"+i)
        dataDF1.select("_c4","_c28","_c32").show()
      }

     //val dataNoDupDF=dataDF1.withColumn("PmtId_rownum", row_number().over(Window.partitionBy("a").orderBy("a").filter($"PmtId_rownum" === 1).drop("PmtId_rownum")

      val dftbl = TBL_NAME + "_df"

      println("Data file Found")
      println("Proceeding with schema file")

      val tblname = TBL_NAME

      println("before write log")

      val completedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm_ss_n").format(LocalDateTime.now)
      println("before write log with date" + completedTime)

      dataDF1.write.options(Map("delimiter" -> "\u0007")).mode(SaveMode.Overwrite).csv("/user/n46995a/output/extract_date=" + completedTime)

      val log2 = ("Completed", completedTime)
      LogList = log2.toString().replace("(", "").replace(")", "") :: LogList

    } catch {
      case e: Throwable =>
        println("File Not Found" + e)
        val failedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
        Exitcode = 1
        val log3 = ("failed", failedTime)
        LogList = log3.toString().replace("(", "").replace(")", "") :: LogList
    } finally {

      spark.stop()

    }


  }
}
