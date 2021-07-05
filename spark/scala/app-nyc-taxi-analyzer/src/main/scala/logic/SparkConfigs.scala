package logic

import org.apache.spark.sql.SparkSession

trait SparkConfigs {

  // SparkSession
  val sparkSession = SparkSession
    .builder()
    .appName("app-nyc-taxi-analyzer")
    .config("spark.master", "local[5]")
    .getOrCreate()

  // Spark Context
  val sparkContext = sparkSession.sparkContext

}
