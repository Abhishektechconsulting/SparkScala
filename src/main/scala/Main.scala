package org.itc.com
import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[1]", "FirstSparkApp")
    println("jenkins test")
    val wordcount= sc.textFile(args(0)).flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((x,y)=>x+y).sortBy(_._2,ascending = false).collect().foreach(println)
  }
}