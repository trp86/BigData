package logic

import org.apache.spark.sql.SparkSession

object BootStrap extends App{

  //Creating spark session
  val spark = SparkSession
    .builder()
    .appName("app-ts-generate-sessionid")
    .config("spark.master", "local[1]")
    .getOrCreate()

  //Execution starting point
  SessionIdGenerator.process(spark)

}
