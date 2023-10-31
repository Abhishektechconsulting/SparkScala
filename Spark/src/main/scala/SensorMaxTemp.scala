package org.itc.com

import org.apache.spark._
import org.apache.spark.streaming._

object SensorMaxTemp {
  def main(args: Array[String]): Unit= {
    val sc = new SparkContext("local[*]","SensorMaxTemp")
    val ssc = new StreamingContext(sc, Seconds(2))

    val stream = ssc.socketTextStream("localhost", 9998)

    val sensorData = stream.map(line => {
      val parts = line.split(" ")
      (parts(1), parts(2).toInt)
    })

    val maxTemp = sensorData.reduceByKey((a, b) => Math.max(a, b))

    maxTemp.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

