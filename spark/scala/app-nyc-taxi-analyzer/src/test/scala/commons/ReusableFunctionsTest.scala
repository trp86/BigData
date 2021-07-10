package commons

import commons.ReusableFunctionsTest._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.scalatest.matchers.must.Matchers
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.apache.spark.sql.types._


class ReusableFunctionsTest extends AnyFunSpec with Matchers with PrivateMethodTester{

  import testSparkSession.implicits._

  describe ("when createDataFrameFromCsvFiles function is invoked") {

    it("should create a spark dataframe from a csv file ") {

      // given
      val csvFileWithPath = someSampleCsvWithPath
      val expectedDf = Seq(("CMT", "50") , ("CHT", "20")).toDF("vendor_id", "total_trips")

      // when
      val actualDf = reusableFunctions.createDataFrameFromCsvFiles(csvFileWithPath)

      // then assert
     actualDf.collect() should contain theSameElementsAs expectedDf.collect()

    }

    it("should throw an exception when spark session object is null") {

      // given
      val csvFileWithPath = someSampleCsvWithPath
      val expectedExceptionMessage = "Spark Session object is null."
      val reusableFunctions = new ReusableFunctions(null)

      // when
      val actualException = intercept[Exception](reusableFunctions.createDataFrameFromCsvFiles(csvFileWithPath))

      // then assert
      expectedExceptionMessage shouldBe actualException.getMessage

    }

    it("should throw an exception when file path is invalid") {

      // given
      val csvFileWithPath = someInvalidPath
      val expectedExceptionMessage = "Path does not exist"

      // when
      val actualException = intercept[AnalysisException](reusableFunctions.createDataFrameFromCsvFiles(csvFileWithPath))

      // then assert
      actualException.getMessage  should startWith (expectedExceptionMessage)

    }

  }

  describe ("when isHeaderMatch function is invoked") {

    it("should return true if header matches ") {

      // given
      val expectedHeader = someHeaderString.split(""",""").toList
      val actualHeader = List("vendor_id", "start_date")


      // when
      val headerMatchStatus = reusableFunctions.isHeaderMatch(expectedHeader, actualHeader)

      // then assert
      headerMatchStatus shouldBe true

    }

    it("should return false if headers are different ") {

      // given
      val expectedHeader = someHeaderString.split(""",""").toList
      val actualHeader = List("vendor_id", "end_date")


      // when
      val headerMatchStatus = reusableFunctions.isHeaderMatch(expectedHeader, actualHeader)

      // then assert
      headerMatchStatus shouldBe false

    }

  }

  describe ("when typecastColumns function is invoked") {

    it("should typecast the columns as per the columnDetails list ") {

      // given
      val inputSchema = StructType(Array(
        StructField("vendor_id", StringType, true),
        StructField("total_cust", StringType, true),
        StructField("fare", StringType, true),
        StructField("maximumtemperature", StringType, true),
        StructField("pickup_datetime", StringType, true),
        StructField("weather_date", StringType, true)
      ))
      val inputDf = testSparkSession.createDataFrame(testSparkContext.parallelize(Seq(Row("CMT", "2", "12.76", "1.3456", "2014-01-09 20:45:25", "1-1-2014"))), inputSchema)
      val colDetailsList = someColumnDetails


      // when
      val actualDf = reusableFunctions.typecastColumns(inputDf, colDetailsList)

      // then assert
      val expectedDf = Seq(("CMT", "2", "12.76", "1.3456", "2014-01-09 20:45:25", "1-1-2014"))
        .toDF("vendor_id", "total_cust", "fare", "maximumtemperature", "pickup_datetime", "weather_date")
         .select('vendor_id, 'total_cust.cast(IntegerType), 'fare.cast(DoubleType), 'maximumtemperature.cast(DecimalType(14, 4)), 'pickup_datetime.cast(TimestampType), to_date('weather_date, "dd-MM-yyyy") as ("weather_date") )

      actualDf.collect() should contain theSameElementsAs expectedDf.collect()
    }

    it("should throw exception if there is unsupported data type for typecasting") {

      // given
      val inputSchema = StructType(Array(
        StructField("vendor_id", StringType, true),
        StructField("total_cust", StringType, true),
        StructField("fare", StringType, true),
        StructField("maximumtemperature", StringType, true),
        StructField("pickup_datetime", StringType, true),
        StructField("weather_date", StringType, true)
      ))
      val inputDf = testSparkSession.createDataFrame(testSparkContext.parallelize(Seq(Row("CMT", "2", "12.76", "1.3456", "2014-01-09 20:45:25", "1-1-2014"))), inputSchema)
      val colDetailsList = List(("someid", "randomdatatype", ""))
      val expectedExceptionMessage = "Unsupported data type for typecasting randomdatatype"


      // when
      val actualException = intercept[Exception](reusableFunctions.typecastColumns(inputDf, colDetailsList))

      // then assert
      expectedExceptionMessage shouldBe actualException.getMessage

    }

  }

  describe ("when filterRecordsHavingNegativeValue function is invoked") {

    it ("should filter out the records having negative value and return back success & error dataframe") {

      // given
      val inputDf = Seq(("CMT", "2", "2.5"), ("CMT", "-1", "-2.5"), ("CMT", "4", "-2.5"), ("CMT", "-1", "5.5"), ("CMT", "0", "6.4"), ("CMT", "1", "0.0"))
        .toDF("vendor_id", "total_cust", "fare")
        .select('vendor_id, 'total_cust.cast(IntegerType), 'fare.cast(DoubleType) )

      val columnNamesForNegativeValueCheck = List ("total_cust", "fare")

      // when
      val (actualSuccessDf, actualErrorDf) = reusableFunctions.filterRecordsHavingNegativeValue(inputDf, columnNamesForNegativeValueCheck)

      // then assert
      val expectedSuccessDf = Seq(("CMT", "2", "2.5"), ("CMT", "0", "6.4"), ("CMT", "1", "0.0"))
        .toDF("vendor_id", "total_cust", "fare")
        .select('vendor_id, 'total_cust.cast(IntegerType), 'fare.cast(DoubleType) )

      val expectedErrorDf = Seq(("CMT", "-1", "-2.5", "total_cust is negative"), ("CMT", "-1", "5.5", "total_cust is negative"), ("CMT", "-1", "-2.5", "fare is negative"), ("CMT", "4", "-2.5", "fare is negative"))
        .toDF("vendor_id", "total_cust", "fare", "rejectReason")
        .select('vendor_id, 'total_cust.cast(IntegerType), 'fare.cast(DoubleType), 'rejectReason )

      // then assert
      actualSuccessDf.collect() should contain theSameElementsAs expectedSuccessDf.collect()
      actualErrorDf.collect() should contain theSameElementsAs expectedErrorDf.collect()

    }

    it ("should throw exception if columns provided for negative check are not present in dataframe") {

      // given
      val inputDf = Seq(("CMT", "2", "2.5")).toDF("vendor_id", "total_cust", "fare").select('vendor_id, 'total_cust.cast(IntegerType), 'fare.cast(DoubleType) )
      val columnNamesForNegativeValueCheck = List ("total_cust", "fare", "trip_distance", "start_time")

      // when
      val actualException = intercept[Exception](reusableFunctions.filterRecordsHavingNegativeValue(inputDf, columnNamesForNegativeValueCheck))

      // then assert
      val expectedExceptionMessage = """trip_distance,start_time not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }

  }

  describe ("when filterRecordsHavingImproperDateTimeValue function is invoked") {

    it ("should filter out the records having improper datetime value and return back success & error dataframe") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31"), ("CMT", "-014-01-09 20:45:25", "2014-01-09 20:52:31"), ("CMT", "2014-01-09 20:45:25", "2333d4-01-09 20:52:31"), ("CMT", "cdef-01-09 20:45:25", "2333d4-01-09 20:52:31"))
        .toDF("vendor_id", "pickup_datetime", "dropoff_datetime")

      val columnNamesForImproperDateTimeCheck = List ("pickup_datetime", "dropoff_datetime")

      // when
      val (actualSuccessDf, actualErrorDf) = reusableFunctions.filterRecordsHavingImproperDateTimeValue(inputDf, columnNamesForImproperDateTimeCheck)

      // then assert
       val expectedSuccessDf = Seq(("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31"))
         .toDF("vendor_id", "pickup_datetime", "dropoff_datetime")

      val expectedErrorDf = Seq(("CMT", "-014-01-09 20:45:25", "2014-01-09 20:52:31", "pickup_datetime datetime format is incorrect"),
         ("CMT", "cdef-01-09 20:45:25", "2333d4-01-09 20:52:31", "pickup_datetime datetime format is incorrect"),
        ("CMT", "2014-01-09 20:45:25", "2333d4-01-09 20:52:31", "dropoff_datetime datetime format is incorrect"),
        ("CMT", "cdef-01-09 20:45:25", "2333d4-01-09 20:52:31", "dropoff_datetime datetime format is incorrect"))
        .toDF("vendor_id", "pickup_datetime", "dropoff_datetime", "rejectReason")

      // then assert
       actualSuccessDf.collect() should contain theSameElementsAs expectedSuccessDf.collect()
       actualErrorDf.collect() should contain theSameElementsAs expectedErrorDf.collect()

    }

    it ("should throw exception if columns provided for improper datetime value check are not present in dataframe") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31"))
        .toDF("vendor_id", "pickup_datetime", "dropoff_datetime")

      val columnNamesForImproperDateTimeCheck = List ("start_date", "pickup_datetime", "end_date", "dropoff_datetime")


      // when
      val actualException = intercept[Exception](reusableFunctions.filterRecordsHavingImproperDateTimeValue(inputDf, columnNamesForImproperDateTimeCheck))

      // then assert
      val expectedExceptionMessage = """start_date,end_date not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }

  }

  describe ("when dataframeColumnsCompare function is invoked") {

    // given
    val inputDf = Seq(("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "20"),
      ("CMT", "2014-01-09 22:45:25", "2014-01-09 20:52:31", "20"),
      ("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "120"),
      ("CMT", "2014-01-09 20:45:25", "2014-01-09 11:52:31", "200"))
      .toDF("vendor_id", "pickup_datetime", "dropoff_datetime", "trip_distance")
      .select('vendor_id, 'pickup_datetime.cast(TimestampType), 'dropoff_datetime.cast(TimestampType), 'trip_distance.cast(DoubleType) )

    it("should return success dataframe which matches the compare expression & error dataframe where the compare expression fails") {

      // given
      val compareExpression = List("""pickup_datetime < dropoff_datetime""", """trip_distance <= 100""")

      // when
      val (actualSuccessDf, actualErrorDf) = reusableFunctions.dataframeColumnsCompare(inputDf, compareExpression)

      // then assert
      val expectedSuccessDf = Seq(("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "20"))
        .toDF("vendor_id", "pickup_datetime", "dropoff_datetime", "trip_distance")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType), 'dropoff_datetime.cast(TimestampType), 'trip_distance.cast(DoubleType) )

      val expectedErrorDf = Seq(("CMT", "2014-01-09 22:45:25", "2014-01-09 20:52:31", "20", "pickup_datetime is not < dropoff_datetime"),
        ("CMT", "2014-01-09 20:45:25", "2014-01-09 11:52:31", "200", "pickup_datetime is not < dropoff_datetime"),
        ("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "120", "trip_distance is not <= 100"),
        ("CMT", "2014-01-09 20:45:25", "2014-01-09 11:52:31", "200", "trip_distance is not <= 100"))
        .toDF("vendor_id", "pickup_datetime", "dropoff_datetime", "trip_distance", "rejectReason")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType), 'dropoff_datetime.cast(TimestampType), 'trip_distance.cast(DoubleType), 'rejectReason)

      // then assert
      actualSuccessDf.collect() should contain theSameElementsAs expectedSuccessDf.collect()
      actualErrorDf.collect() should contain theSameElementsAs expectedErrorDf.collect()


    }

    it ("should throw exception if columns provided for columns compare are not present in dataframe") {

      // given
      val compareExpression = List("""pickup_datetime < end_datetime""", """trip_distance <= 100""")

      // when
      val actualException = intercept[Exception](reusableFunctions.dataframeColumnsCompare(inputDf, compareExpression))

      // then assert
      val expectedExceptionMessage = """end_datetime not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }

    it("should throw exception if a string column has comparision operator apart from = and !=") {

      // given
      val compareExpression = List("""vendor_id < dropoff_datetime""", """trip_distance <= 100""")

      // when
      val actualException = intercept[Exception](reusableFunctions.dataframeColumnsCompare(inputDf, compareExpression))

      // then assert
      val expectedExceptionMessage = """For string datatype compare operator could only be = and !="""
      expectedExceptionMessage shouldBe actualException.getMessage
    }

    it("""should throw exception if comparision operator is apart from =,!=,>,<,>=,<=""") {

      // given
      val compareExpression = List("""pickup_datetime < dropoff_datetime""", """trip_distance && 100""")

      // when
      val actualException = intercept[Exception](reusableFunctions.dataframeColumnsCompare(inputDf, compareExpression))

      // then assert
      val expectedExceptionMessage = """Invalid comparison operator!!!!"""
      expectedExceptionMessage shouldBe actualException.getMessage
    }

  }

  describe ("when checkIfColumnsExistInDataFrame function is invoked") {

    it ("should throw exception if columns provided are not present in dataframe") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "20"),
        ("CMT", "2014-01-09 22:45:25", "2014-01-09 20:52:31", "20"),
        ("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "120"),
        ("CMT", "2014-01-09 20:45:25", "2014-01-09 11:52:31", "200"))
        .toDF("vendor_id", "pickup_datetime", "dropoff_datetime", "trip_distance")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType), 'dropoff_datetime.cast(TimestampType), 'trip_distance.cast(DoubleType) )

      val columnsList = List ("snowdepth", "snowfall", "vendor_id")

      // when
      val actualException = intercept[Exception](reusableFunctions.checkIfColumnsExistInDataFrame(inputDf, columnsList))

      // then assert
      val expectedExceptionMessage = """snowdepth,snowfall not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }

    it ("should not throw exception and return Unit if columns provided are present in dataframe") {

      // given
      val inputDf = Seq(("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "20"),
        ("CMT", "2014-01-09 22:45:25", "2014-01-09 20:52:31", "20"),
        ("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "120"),
        ("CMT", "2014-01-09 20:45:25", "2014-01-09 11:52:31", "200"))
        .toDF("vendor_id", "pickup_datetime", "dropoff_datetime", "trip_distance")
        .select('vendor_id, 'pickup_datetime.cast(TimestampType), 'dropoff_datetime.cast(TimestampType), 'trip_distance.cast(DoubleType) )

      val columnsList = List ("pickup_datetime", "dropoff_datetime", "vendor_id")

      // when
      val actual = reusableFunctions.checkIfColumnsExistInDataFrame(inputDf, columnsList)

      // then assert
      actual shouldBe () // Unit

    }

  }

  describe ("when renameColumnInDataFrame function is invoked") {

    it ("should rename a column in dataframe if column exists") {

      // given
      val inputDf = Seq(("CMT")).toDF("vendor_id")

      // when
      val actualDF = reusableFunctions.renameColumnInDataFrame(inputDf, "vendor_id", "vendorid")

      // then assert
      val expectedDF = Seq(("CMT")).toDF("vendorid")
      actualDF.collect() should contain theSameElementsAs expectedDF.collect()
      expectedDF.columns.toList shouldBe List("vendorid")


    }

    it ("should throw exception if columns provided to rename are not present in dataframe") {

      // given
      val inputDf = Seq(("CMT")).toDF("vendor_id")

      // when
      val actualException = intercept[Exception](reusableFunctions.renameColumnInDataFrame(inputDf, "vendorId", "vendorid"))

      // then assert
      val expectedExceptionMessage = """vendorId not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage


    }

  }




}

object ReusableFunctionsTest {

  System.setProperty("hadoop.home.dir", """C:\Work\winutil\""")

   val testSparkSession = SparkSession
    .builder()
    .appName("app-nyc-taxi-analyzer-test")
    .config("spark.master", "local[1]")
    .getOrCreate()

  val testSparkContext: SparkContext = testSparkSession.sparkContext

  val reusableFunctions = new ReusableFunctions(testSparkSession)

  val someSampleCsvWithPath = """src/test/resources/inputdata/someSample.csv"""
  val someInvalidPath = """some/invalid/path"""
  val someHeaderString = """vendor_id,start_date"""
  val someColumnDetails: List[(String, String, String)] = """vendor_id:string|total_cust:int|fare:double|maximumtemperature:decimal:(14,4)|pickup_datetime:datetime|weather_date:date:dd-MM-yyyy""".split("""\|""").map(x => {
    val y = x.split(""":""")
    if (y.length == 3) (y(0), y(1), y(2)) else (y(0), y(1), "")
  }).toList


}
