package logic

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.log4j.PropertyConfigurator

object BootStrap extends App{

  val log = Logger.getLogger(BootStrap.getClass)
  PropertyConfigurator.configure("src/main/resources/conf/log4j.properties")

  //Creating spark session
  val spark = SparkSession
    .builder()
    .appName("app-ts-generate-sessionid")
    .config("spark.master", "local[1]")
    .getOrCreate()

  log.info("Spark session object created successfully.")

  //Execution starting point
 SessionIdGenerator.process(spark)
}
