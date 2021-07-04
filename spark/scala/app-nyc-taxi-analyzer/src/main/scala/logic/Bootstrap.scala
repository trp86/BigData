package logic

import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.sql.SparkSession

object Bootstrap extends App{



  val log = Logger.getLogger(Bootstrap.getClass)
  PropertyConfigurator.configure("src/main/resources/conf/log4j.properties")


  //Creating spark session
  val spark = SparkSession
    .builder()
    .appName("app-nyc-taxi-analyzer")
    .config("spark.master", "local[1]")
    .getOrCreate()

  log.info("Spark session object created successfully.")

  val sc = spark.sparkContext

  // create DataFrame from scala Seq. It can infer schema for you.
  // val df1 = spark.createDataFrame(Seq((1, "andy", 20, "USA"), (2, "jeff", 23, "China"), (3, "james", 18, "USA"))).toDF("id", "name", "age", "country")
  // df1.printSchema
  // df1.show()

  val df1 = spark.read.option("header", true).csv("""/home/code-bind/Desktop/nyctaxianalysis/yellow_tripdata_subset_2014-01.csv""")

  // df1.show(20)
  // df1.printSchema()

  df1.write.option("header", true).csv("""/home/code-bind/Desktop/nyctaxianalysis/temp""")
}
