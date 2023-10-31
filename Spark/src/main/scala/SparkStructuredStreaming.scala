package org.itc.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object SparkStructuredStreaming {
  def main(args:Array[String]) : Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name","SparkStructuredStreamingDemo")
    sparkConf.set("spark.master","local[1]")
    sparkConf.set("spark.streaming.stopGracefullyonShutdown","true")
    val spark  = SparkSession.builder().config(sparkConf).getOrCreate()

    val linesdf = spark.readStream.format("socket").option("host","localhost").option("port", 9998).load()

    import spark.implicits._
    val wordsdf = linesdf.as[String].flatMap(_.split(" "))
    val wordcount = wordsdf.groupBy("value").count()
    val consumer1 = wordcount.writeStream.outputMode("complete").format("console")
      .option("checkpoint", "checkpointloc1").trigger(Trigger.ProcessingTime("2 seconds")).start().awaitTermination()

  }


}
