package org.itc.com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StructuredSensorMaxTemp {
  def main(args: Array[String]): Unit= {
    val spark = SparkSession
      .builder
      .config("spark.master","local[1]")
      .appName("StructuredSensorMaxTemp")
      .getOrCreate()

    val stream_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9998)
      .load()

    val split_df = stream_df.selectExpr(
        "split(value, ' ')[0] as date_time",
        "split(value, ' ')[1] as sensorNo",
        "split(value, ' ')[2] as temp")
      .withColumn("temp", col("temp").cast("integer"))

    val max_temp_df = split_df.groupBy("sensorNo").agg(max("temp").alias("max_temp"))

    val query = max_temp_df.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}

