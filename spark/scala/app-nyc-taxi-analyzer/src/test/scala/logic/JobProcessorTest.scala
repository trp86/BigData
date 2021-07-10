package logic

import commons.ReusableFunctions
import commons.ReusableFunctionsTest._
import logic.JobProcessorTest.columnListOfJoinDf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class JobProcessorTest extends AnyFunSpec with Matchers with PrivateMethodTester{


  describe ("when process function is invoked") {

    it("should process trip and weather data and persist the data") {

      // when
      JobProcessor.process(testSparkSession)
      val actualDf = testSparkSession.read.parquet("src/main/resources/outbound/processeddata/success/")

      // then assert
      actualDf.count() shouldBe 999
      actualDf.columns.toList shouldBe columnListOfJoinDf

    }
  }
}

object JobProcessorTest {

  System.setProperty("hadoop.home.dir", """C:\Work\winutil\""")

  val testSparkSession = SparkSession
    .builder()
    .appName("app-nyc-taxi-analyzer-test")
    .config("spark.master", "local[1]")
    .getOrCreate()

  val testSparkContext: SparkContext = testSparkSession.sparkContext

  val reusableFunctions = new ReusableFunctions(testSparkSession)

  val columnListOfJoinDf = List("vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "pickup_longitude", "pickup_latitude", "rate_code", "store_and_fwd_flag", "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount", "trip_date", "trip_hour", "trip_day_of_week", "maximumtemperature", "minimumtemperature", "averagetemperature", "precipitation", "snowfall", "snowdepth", "temperature_condition", "snowfall_condition", "snowdepth_condition", "rain_condition", "weather_date")

}


