package com.worldpay.spark


import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{udf, _}


object ecommBDAIngst {

  def reformat_date(time_stamp1: String): String = {
    val time_stamp = Option(time_stamp1).getOrElse(return " ")
    val time_stamp_lng = time_stamp.length()
    if(time_stamp == null && time_stamp.isEmpty ) {
      val a = " "
      a
    }else{
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
        val b = " "
        b
      }
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

      val job_properties = job_input.collect().toList.flatMap(x => x.split('|')).grouped(2).collect { case List(k, v) => k -> v }.toMap

      val TBL_NAME = job_properties("TBL_NAME")

      val TIME_COLS_ARRAY = job_properties("TIME_COLS").split(",")

      val formatting = udf(reformat_date _)

      println("Processing source file  started")

      var srcDF = spark.read
        .options(Map("delimiter" -> "\u0007", "ignoreLeadingWhiteSpace" -> "True", "ignoreTrailingWhiteSpace" -> "True", "multiline" -> job_properties("MVAL"), "quote" -> "\'","escape" -> "\'"))
        .csv((job_properties("SRC_FILE_PATH") + "/" + job_properties("SRC_FILE_NM")))

      val sourceCount = srcDF.count()

      for (i <- TIME_COLS_ARRAY) {
        var tempDF = srcDF.withColumn(i, formatting(col(i)))
        srcDF = tempDF
      }
      val dedupKey=job_properties("Dedup_Key").split(",").toSeq.map(x => col(x.trim))
      val dedupOrderKey=job_properties("Dedup_order_Key").split(",").toSeq.map(x => col(x.trim).desc)

      import spark.sqlContext.implicits._
      val dataNoDupDF = srcDF
        .withColumn("PmtId_rownum", row_number()
          .over(Window
            .partitionBy(dedupKey:_*)
            .orderBy(dedupOrderKey:_*)
          )
        )
        .filter($"PmtId_rownum" === 1)
        .drop("PmtId_rownum")

      val dedupCount=dataNoDupDF.count()

      val reportData = Seq((job_properties("SRC_FILE_NM"),dedupCount,sourceCount)).toDF()

      val coalesceCount = (dedupCount/job_properties("PART_CAL_VAL").toLong +1).toInt


      dataNoDupDF.coalesce(coalesceCount).write.options(Map("delimiter" -> "\u0007")).mode(SaveMode.Overwrite).csv((job_properties("TAR_FILE_PATH") + "/" + job_properties("TAR_FILE_NM")))
      reportData.write.option("header","false").mode(SaveMode.Overwrite).csv((job_properties("REPORT_FILE_PATH") + "/" + job_properties("REPORT_FILE_NM")))
      println("Processing source file completed")
    } catch {
      case e: Throwable =>
        println(e)
        Exitcode = 1
    } finally {
      spark.stop()
    }
  }
}
