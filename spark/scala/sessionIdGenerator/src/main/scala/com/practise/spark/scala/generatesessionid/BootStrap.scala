package com.practise.spark.scala.generatesessionid


import org.apache.spark.sql.SparkSession
object BootStrap extends App{


  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local[1]")
    .getOrCreate()
  import spark.implicits._

  val df = Seq(1).toDF
df.show

}
