package commons

import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.matchers.must.Matchers
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.apache.spark.sql.types._

class TripDataReusableFunctionsTest extends AnyFunSpec with Matchers with PrivateMethodTester{


  import TripDataReusableFunctionsTest._
  import testSparkSession.implicits._

  describe ("when addTripDateColumn function is invoked") {

    // placeholder for private method
    val addTripDateColumnPlaceHolder = PrivateMethod[DataFrame](Symbol("addTripDateColumn"))

    it("should add trip_date column to input dataframe ") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25"),
        ("CMT", "2014-01-09 22:45:25"))
        .toDF("vendor_id", "pickup_datetime")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType))

      // when
      val actualDf = tripDatareusableFunctions invokePrivate addTripDateColumnPlaceHolder(inputDf)

      // then assert
      val expectedDf = Seq(("CMT", "2014-01-09 20:45:25", "2014-01-09"),
        ("CMT", "2014-01-09 22:45:25", "2014-01-09"))
        .toDF("vendor_id", "pickup_datetime", "trip_date")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType), 'trip_date.cast(DateType))

      actualDf.collect() should contain theSameElementsAs expectedDf.collect()
    }

    it("should throw exception if pickup_datetime column is not present in dataframe") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25"),
        ("CMT", "2014-01-09 22:45:25"))
        .toDF("vendor_id", "start_datetime")
        .select('vendor_id, 'start_datetime.cast(TimestampType))

      // when
      val actualException = intercept[Exception](tripDatareusableFunctions invokePrivate addTripDateColumnPlaceHolder(inputDf))

      // then assert
      val expectedExceptionMessage = """pickup_datetime not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }

  }

  describe ("when addTripHourOfDayColumn function is invoked") {

    // placeholder for private method
    val addTripHourOfDayColumnPlaceHolder = PrivateMethod[DataFrame](Symbol("addTripHourOfDayColumn"))

    it("should add trip_hour column to input dataframe ") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25"),
        ("CMT", "2014-01-09 22:45:25"))
        .toDF("vendor_id", "pickup_datetime")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType))

      // when
      val actualDf = tripDatareusableFunctions invokePrivate addTripHourOfDayColumnPlaceHolder(inputDf)

      // then assert
      val expectedDf = Seq(("CMT", "2014-01-09 20:45:25", 20),
        ("CMT", "2014-01-09 22:45:25", 22))
        .toDF("vendor_id", "pickup_datetime", "trip_hour")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType), 'trip_hour)

      actualDf.collect() should contain theSameElementsAs expectedDf.collect()
    }

     it("should throw exception if pickup_datetime column is not present in dataframe") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25"),
        ("CMT", "2014-01-09 22:45:25"))
        .toDF("vendor_id", "start_datetime")
        .select('vendor_id, 'start_datetime.cast(TimestampType))

      // when
      val actualException = intercept[Exception](tripDatareusableFunctions invokePrivate addTripHourOfDayColumnPlaceHolder(inputDf))

      // then assert
      val expectedExceptionMessage = """pickup_datetime not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }

  }

  describe ("when addTripDayOfWeekColumn function is invoked") {

    // placeholder for private method
    val addTripDayOfWeekColumnPlaceHolder = PrivateMethod[DataFrame](Symbol("addTripDayOfWeekColumn"))

    it("should add trip_day_of_week column to input dataframe ") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25"),
        ("CMT", "2014-01-10 22:45:25"),
        ("CMT", "2014-01-11 22:45:25"))
        .toDF("vendor_id", "pickup_datetime")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType))

      // when
      val actualDf = tripDatareusableFunctions invokePrivate addTripDayOfWeekColumnPlaceHolder(inputDf)

      // then assert
      val expectedDf = Seq(("CMT", "2014-01-09 20:45:25", 5),
        ("CMT", "2014-01-10 22:45:25", 6),
        ("CMT", "2014-01-11 22:45:25", 7))
        .toDF("vendor_id", "pickup_datetime", "trip_day_of_week")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType), 'trip_day_of_week)

       actualDf.collect() should contain theSameElementsAs expectedDf.collect()
    }

    it("should throw exception if pickup_datetime column is not present in dataframe") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25"),
        ("CMT", "2014-01-09 22:45:25"))
        .toDF("vendor_id", "start_datetime")
        .select('vendor_id, 'start_datetime.cast(TimestampType))

      // when
      val actualException = intercept[Exception](tripDatareusableFunctions invokePrivate addTripDayOfWeekColumnPlaceHolder(inputDf))

      // then assert
      val expectedExceptionMessage = """pickup_datetime not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }

}

  describe ("when addAdditionalColumns function is invoked") {

    // placeholder for private method
    val addAdditionalColumnsPlaceHolder = PrivateMethod[DataFrame](Symbol("addAdditionalColumns"))

    it("should add trip_date,trip_hour,trip_day_of_week columns to input dataframe ") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25"),
        ("CMT", "2014-01-10 22:45:25"),
        ("CMT", "2014-01-11 22:45:25"))
        .toDF("vendor_id", "pickup_datetime")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType))

      // when
      val actualDf = tripDatareusableFunctions invokePrivate addAdditionalColumnsPlaceHolder(inputDf)


      // then assert
      val expectedDf = Seq(("CMT", "2014-01-09 20:45:25", "2014-01-09", 20, 5),
        ("CMT", "2014-01-10 22:45:25", "2014-01-10", 22, 6),
        ("CMT", "2014-01-11 22:45:25", "2014-01-11", 22, 7))
        .toDF("vendor_id", "pickup_datetime", "trip_date", "trip_hour", "trip_day_of_week")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType), 'trip_date.cast(DateType), 'trip_hour, 'trip_day_of_week)

       actualDf.collect() should contain theSameElementsAs expectedDf.collect()
    }

    it("should throw exception if pickup_datetime column is not present in dataframe") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25"),
        ("CMT", "2014-01-09 22:45:25"))
        .toDF("vendor_id", "start_datetime")
        .select('vendor_id, 'start_datetime.cast(TimestampType))

      // when
      val actualException = intercept[Exception](tripDatareusableFunctions invokePrivate addAdditionalColumnsPlaceHolder(inputDf))

      // then assert
      val expectedExceptionMessage = """pickup_datetime not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }

  }

  describe ("when performDQandAddColumns function is invoked") {

    it ("should perform data quality & add additional columns and return success & error dataframe") {

      // given
      val inputDf = reusableFunctions.createDataFrameFromCsvFiles("""src/test/resources/inputdata/yellow_tripdata_subset_2014-01.csv""")

     // when
      val (actualSuccessDf, actualErrorDf) = tripDatareusableFunctions.performDQandAddColumns(inputDf)

      // concatenating all columns
      val actualSuccessDfWithConcatCol = actualSuccessDf.withColumn("concatCol", concat_ws("|", col("vendor_id"), col("pickup_datetime"), col("dropoff_datetime"),
        col("passenger_count"), col("trip_distance"), col("pickup_longitude"), col("pickup_latitude"),
        col("rate_code"), col("store_and_fwd_flag"), col("dropoff_longitude"), col("dropoff_latitude"),
        col("payment_type"), col("fare_amount"), col("surcharge"), col("mta_tax"), col("tip_amount"),
        col("tolls_amount"), col("total_amount"), col("trip_date"), col("trip_hour"), col("trip_day_of_week")))

      val actualErrorDfWithConcatCol = actualErrorDf.withColumn("concatCol", concat_ws("|", col("vendor_id"), col("pickup_datetime"), col("dropoff_datetime"),
        col("passenger_count"), col("trip_distance"), col("pickup_longitude"), col("pickup_latitude"),
        col("rate_code"), col("store_and_fwd_flag"), col("dropoff_longitude"), col("dropoff_latitude"),
        col("payment_type"), col("fare_amount"), col("surcharge"), col("mta_tax"), col("tip_amount"),
        col("tolls_amount"), col("total_amount"), col("rejectReason")))

      val actualSuccessDfConcatString = actualSuccessDfWithConcatCol.select('concatCol).collect() (0).toString()
      val actualErrorDfConcatString = actualErrorDfWithConcatCol.select('concatCol).collect() (0).toString()

      // then assert
      val expectedSuccessDfString = """[CMT|2014-01-09 20:45:25|2014-01-09 20:52:31|1|0.7|-73.994770000000003|40.736828000000003|1|N|-73.982226999999995|40.731789999999997|CRD|6.5|0.5|0.5|1.4|0.0|8.9|2014-01-09|20|5]"""
      val expectedErrorDfString = """[CMT|2014-01-09 20:45:25|2014-01-09 20:52:31|1|5000.0|-73.994770000000003|40.736828000000003|1|N|-73.982226999999995|40.731789999999997|CRD|6.5|0.5|0.5|1.4|0.0|8.9|trip_distance is not <= 100]"""

      actualSuccessDfConcatString shouldBe expectedSuccessDfString
      actualErrorDfConcatString shouldBe expectedErrorDfString

    }



  }



}

object TripDataReusableFunctionsTest {

  System.setProperty("hadoop.home.dir", """C:\Work\winutil\""")

  val testSparkSession = SparkSession
    .builder()
    .appName("app-nyc-taxi-analyzer-test")
    .config("spark.master", "local[1]")
    .getOrCreate()

  val testSparkContext = testSparkSession.sparkContext

  val tripDatareusableFunctions = new TripDataReusableFunctions(testSparkSession)
  val reusableFunctions = new ReusableFunctions(testSparkSession)

}
