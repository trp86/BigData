package commons


import commons.ReusableFunctionsTest._
import logic.Test.sparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.matchers.must.Matchers
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class ReusableFunctionsTest extends AnyFunSpec with Matchers with PrivateMethodTester{

  import sparkSession.implicits._

  describe("when createDataFrameFromCsvFiles function is invoked") {

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
}

object ReusableFunctionsTest {

   val testSparkSession = SparkSession
    .builder()
    .appName("app-nyc-taxi-analyzer-test")
    .config("spark.master", "local[1]")
    .getOrCreate()

  val testSparkContext: SparkContext = testSparkSession.sparkContext

  val reusableFunctions = new ReusableFunctions(testSparkSession)

  val someSampleCsvWithPath = """src/test/resources/inputdata/"""
  val someInvalidPath = """some/invalid/path"""
  val someHeaderString = """vendor_id,start_date"""
}
