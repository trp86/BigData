package logic

import org.apache.spark.sql.SparkSession

object BootStrap extends App{

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local[1]")
    .getOrCreate()
  import spark.implicits._

  val df = Seq(("Tim",23),("Tom",34)).toDF("name","age")
  println("DataFrame Count:---"+df.count())

  df.show

}
