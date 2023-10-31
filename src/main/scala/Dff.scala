package org.itc.com

import org.apache
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object Dff  extends App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "dffDemo")
  sparkConf.set("spark.master", "local[1]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  case class  orderdata(orderid:Int, orderdate:String, custid:Int, status:String)

  val ordersDDL = "orderid Int, orderdate String, custid Int, status String"

  val orderdf = spark.read.option("header", true)

  //val orderdf = spark.read.option("header", true).option("inferSchema", true).csv("C:/Users/Abhishek Praharaj/Downloads/orders.csv")
  /*val orderSchema = StructType(List(StructField("orderid", IntegerType, true), StructField("orderdate", StringType, true),
  StructField("custid", IntegerType, true), StructField("status", StringType, true)))
  val orderdf = spark.read.option("header", true).schema(orderSchema).csv("C:/Users/Abhishek Praharaj/Downloads/orders.csv")
  orderdf.show(5)

  orderdf.where("custid < 10000").show(3)
  orderdf.where("custid< 10000").groupBy("status").agg(functions.min("orderid"), functions.max("orderid")).show(3)
  println("min orderid" + orderdf.agg(functions.min("orderid")).head().get(0))*/
}
