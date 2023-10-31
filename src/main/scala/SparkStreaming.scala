package org.itc.com

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.Logger.getLogger



object SparkStreaming {
  def main(args: Array[String]): Unit = {

     val sc = new SparkContext("local[*]","SparkStreamingdemo")
     val ssc = new StreamingContext(sc, Seconds(2))
     Logger.getLogger("org").setLevel(Level.ERROR)

     val lines =  ssc.socketTextStream("localhost", 9998)

     val wordcount = lines.flatMap(x => x.toLowerCase().split(" "))
      .map(x => (x,1))
      .reduceByKey((x,y)=>x+y)

    wordcount.print()
    ssc.start()
    ssc.awaitTermination()


  }

}
