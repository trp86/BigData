package commons

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class WeatherDataReusableFunctionsTest extends AnyFunSpec with Matchers with PrivateMethodTester {

  import WeatherDataReusableFunctionsTest._
  import testSparkSession.implicits._

  describe("when replaceTwithNegligibleValues function is invoked") {

    // placeholder for private method
    val replaceTwithNegligibleValuesPlaceHolder = PrivateMethod[DataFrame](Symbol("replaceTwithNegligibleValues"))

    // given
    val inputDf = Seq(("T", "medium"), ("0.5", "violent")).toDF("snowdepth", "weathertype")

    it("should replace T with 0.0001 in input dataframe columns having data type as decimal") {

      // given
      val columnDetails = List(("snowdepth", "decimal", "(14,4)"), ("weathertype", "string", ""))

      // when
      val actualDf = weatherDataReusableFunctions invokePrivate replaceTwithNegligibleValuesPlaceHolder(inputDf, columnDetails)

      // then assert
      val expectedDf = Seq(("0.0001", "medium"), ("0.5", "violent")).toDF("snowdepth", "weathertype")
      actualDf.collect() should contain theSameElementsAs expectedDf.collect()
    }

    it("should throw exception if some column is not present in dataframe") {

      // given
      val columnDetails = List(("snowdepth", "decimal", "(14,4)"), ("weathertype", "string", ""), ("snowfall", "decimal", "(14,4)"))

      // when
      val actualException = intercept[Exception](weatherDataReusableFunctions invokePrivate replaceTwithNegligibleValuesPlaceHolder(inputDf, columnDetails))

      // then assert
      val expectedExceptionMessage = """snowfall not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }

  }

  describe("when addTemperatureConditionColumn function is invoked") {

    // placeholder for private method
    val addTemperatureConditionColumnPlaceHolder = PrivateMethod[DataFrame](Symbol("addTemperatureConditionColumn"))

    it("should create additional column temperature_condition in the existing dataframe") {

      // given
      val inputDf = Seq((-22), (28), (32), (44), (59), (62), (77), (84), (95), (105)).toDF("averagetemperature")

      // when
      val actualDf = weatherDataReusableFunctions invokePrivate addTemperatureConditionColumnPlaceHolder(inputDf)

      // then assert
      val expectedDf = Seq((-22, "verycold"), (28, "verycold"), (32, "cold"), (44, "cold"), (59, "normal"), (62, "normal"), (77, "hot"), (84, "hot"), (95, "veryhot"), (105, "veryhot")).toDF("averagetemperature", "temperature_condition")
      actualDf.collect() should contain theSameElementsAs expectedDf.collect()
    }

    it("should throw exception if averagetemperature column is not present in dataframe") {

      // given
      val inputDf = Seq((-22), (28), (32), (44), (59), (62), (77), (84), (95), (105)).toDF("maxtemperature")

      // when
      val actualException = intercept[Exception](weatherDataReusableFunctions invokePrivate addTemperatureConditionColumnPlaceHolder(inputDf))

      // then assert
      val expectedExceptionMessage = """averagetemperature not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }

  }

  describe("when addSnowFallConditionColumn function is invoked") {

    // placeholder for private method
    val addSnowFallConditionColumnPlaceHolder = PrivateMethod[DataFrame](Symbol("addSnowFallConditionColumn"))

    it("should create additional column snowfall_condition in the existing dataframe") {

      // given
      val inputDf = Seq((-2), (0.0001), (0.7), (2), (4), (10), (15), (30)).toDF("snowfall").select('snowfall.cast(DecimalType(14, 4)))

      // when
      val actualDf = weatherDataReusableFunctions invokePrivate addSnowFallConditionColumnPlaceHolder(inputDf)

      // then assert
      val expectedDf = Seq(("-2", "nosnow"), ("0.0001", "nosnow"), ("0.7", "moderate"), ("2", "moderate"), ("4", "heavy"), ("10", "heavy"), ("15", "violent"), ("30", "violent")).toDF("snowfall", "snowfall_condition").select('snowfall.cast(DecimalType(14, 4)), 'snowfall_condition)
      actualDf.collect() should contain theSameElementsAs expectedDf.collect()
    }

    it("should throw exception if snowfall column is not present in dataframe") {

      // given
      val inputDf = Seq((-2), (0.0001), (0.7), (2), (4), (10), (15), (30)).toDF("snowdepth").select('snowdepth.cast(DecimalType(14, 4)))


      // when
      val actualException = intercept[Exception](weatherDataReusableFunctions invokePrivate addSnowFallConditionColumnPlaceHolder(inputDf))

      // then assert
      val expectedExceptionMessage = """snowfall not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }
  }

  describe("when addSnowDepthConditionColumn function is invoked") {

      // placeholder for private method
      val addSnowDepthConditionColumnPlaceHolder = PrivateMethod[DataFrame](Symbol("addSnowDepthConditionColumn"))

      it("should create additional column snowdepth_condition in the existing dataframe") {

        // given
        val inputDf = Seq((-2), (0.0001), (0.7), (2), (4), (10), (15), (30)).toDF("snowdepth").select('snowdepth.cast(DecimalType(14, 4)))

        // when
        val actualDf = weatherDataReusableFunctions invokePrivate addSnowDepthConditionColumnPlaceHolder(inputDf)

        // then assert
        val expectedDf = Seq(("-2", "nosnow"), ("0.0001", "nosnow"), ("0.7", "moderate"), ("2", "moderate"), ("4", "heavy"), ("10", "heavy"), ("15", "violent"), ("30", "violent")).toDF("snowdepth", "snowdepth_condition").select('snowdepth.cast(DecimalType(14, 4)), 'snowdepth_condition)
        actualDf.collect() should contain theSameElementsAs expectedDf.collect()
      }

      it("should throw exception if snowdepth column is not present in dataframe") {

        // given
        val inputDf = Seq((-2), (0.0001), (0.7), (2), (4), (10), (15), (30)).toDF("snowfall").select('snowfall.cast(DecimalType(14, 4)))


        // when
        val actualException = intercept[Exception](weatherDataReusableFunctions invokePrivate addSnowDepthConditionColumnPlaceHolder(inputDf))

        // then assert
        val expectedExceptionMessage = """snowdepth not present in dataframe."""
        expectedExceptionMessage shouldBe actualException.getMessage

      }

    }

  describe("when addRainConditionColumn function is invoked") {

      // placeholder for private method
      val addRainConditionColumnPlaceHolder = PrivateMethod[DataFrame](Symbol("addRainConditionColumn"))

      it("should create additional column rain_condition in the existing dataframe") {

        // given
        val inputDf = Seq(("-1"), ("0"), ("0.1"), ("0.25"), ("0.3"), ("1.5"), ("12"), ("2.5")).toDF("precipitation").select('precipitation.cast(DecimalType(14, 4)))

        // when
        val actualDf = weatherDataReusableFunctions invokePrivate addRainConditionColumnPlaceHolder(inputDf)

        // then assert
        val expectedDf = Seq(("-1", "norain"), ("0", "norain"), ("0.1", "moderate"), ("0.25", "moderate"), ("0.3", "heavy"), ("1.5", "heavy"), ("12", "violent"), ("2.5", "violent")).toDF("precipitation", "rain_condition").select('precipitation.cast(DecimalType(14, 4)), 'rain_condition)
        actualDf.collect() should contain theSameElementsAs expectedDf.collect()
      }

      it("should throw exception if precipitation column is not present in dataframe") {

        // given
        val inputDf = Seq((-2), (0.0001), (0.7), (2), (4), (10), (15), (30)).toDF("snowfall").select('snowfall.cast(DecimalType(14, 4)))

        // when
        val actualException = intercept[Exception](weatherDataReusableFunctions invokePrivate addRainConditionColumnPlaceHolder(inputDf))

        // then assert
        val expectedExceptionMessage = """precipitation not present in dataframe."""
        expectedExceptionMessage shouldBe actualException.getMessage

      }

    }

  describe("when addAdditionalColumns function is invoked") {

    // placeholder for private method
    val addAdditionalColumnsPlaceHolder = PrivateMethod[DataFrame](Symbol("addAdditionalColumns"))

    it("should create additional columns rain_condition, snowfall_condition, snowdepth_condition,temperature_condition  in the existing dataframe") {

      // given
      val inputDf = Seq((-1, 0.5, 0.5, 45 ))
        .toDF("precipitation", "snowfall", "snowdepth", "averagetemperature") .select('precipitation.cast(DecimalType(14, 4)), 'snowfall.cast(DecimalType(14, 4)),
        'snowdepth.cast(DecimalType(14, 4)), 'averagetemperature.cast(DecimalType(14, 4)))

      // when
      val actualDf = weatherDataReusableFunctions invokePrivate addAdditionalColumnsPlaceHolder(inputDf)

      // then assert
      val expectedDf = Seq(("-1", "0.5", "0.5", "45", "cold", "moderate", "moderate", "norain"))
        .toDF("precipitation", "snowfall", "snowdepth", "averagetemperature", "temperature_condition",
          "snowfall_condition", "snowdepth_condition", "rain_condition")
        .select('precipitation.cast(DecimalType(14, 4)), 'snowfall.cast(DecimalType(14, 4)),
          'snowdepth.cast(DecimalType(14, 4)), 'averagetemperature.cast(DecimalType(14, 4)),
          'temperature_condition, 'snowfall_condition, 'snowdepth_condition, 'rain_condition)

      actualDf.collect() should contain theSameElementsAs expectedDf.collect()
    }

    it("should throw exception if columns like averagetemperature, snowfall, snowdepth, precipitation is not present in dataframe") {

      // given
      val inputDf = Seq((-2), (0.0001), (0.7), (2), (4), (10), (15), (30)).toDF("snowfall").select('snowfall.cast(DecimalType(14, 4)))

      // when
      val actualException = intercept[Exception](weatherDataReusableFunctions invokePrivate addAdditionalColumnsPlaceHolder(inputDf))

      // then assert
      val expectedExceptionMessage = """averagetemperature not present in dataframe."""
      expectedExceptionMessage shouldBe actualException.getMessage

    }

  }

  describe("when performDQandAddColumns function is invoked") {

    it("should create additional columns rain_condition, snowfall_condition, snowdepth_condition,temperature_condition  in the existing dataframe") {

     // given
      val inputDf = Seq (("1-1-2014", "42", "34", "38", "0", "0", "0"), ("2-1-2014", "20", "32", "36", "0", "0", "0")).
        toDF("weather_date", "maximumtemperature", "minimumtemperature", "averagetemperature", "precipitation", "snowfall", "snowdepth")

      // when
      val (actualSuccessDf, actualErrorDf) = weatherDataReusableFunctions.performDQandAddColumns(inputDf)

      // then assert
      val expectedSuccessDf = Seq (("2014-01-01", "42", "34", "38", "0", "0", "0", "cold", "nosnow", "nosnow", "norain")).
        toDF("weather_date", "maximumtemperature", "minimumtemperature", "averagetemperature", "precipitation", "snowfall", "snowdepth",
          "temperature_condition", "snowfall_condition", "snowdepth_condition", "rain_condition")
        .select(
          'weather_date.cast(DateType), 'maximumtemperature.cast(DecimalType(14, 4)), 'minimumtemperature.cast(DecimalType(14, 4)),
            'averagetemperature.cast(DecimalType(14, 4)), 'precipitation.cast(DecimalType(14, 4)), 'snowfall.cast(DecimalType(14, 4)),
      'snowdepth.cast(DecimalType(14, 4)), 'temperature_condition, 'snowfall_condition, 'snowdepth_condition, 'rain_condition
        )

      val expectedErrorDf = Seq (("2014-01-02", "20", "32", "36", "0", "0", "0", "minimumtemperature is not < maximumtemperature")).
        toDF("weather_date", "maximumtemperature", "minimumtemperature", "averagetemperature", "precipitation", "snowfall", "snowdepth",
          "rejectReason")
        .select(
          'weather_date.cast(DateType), 'maximumtemperature.cast(DecimalType(14, 4)), 'minimumtemperature.cast(DecimalType(14, 4)),
          'averagetemperature.cast(DecimalType(14, 4)), 'precipitation.cast(DecimalType(14, 4)), 'snowfall.cast(DecimalType(14, 4)),
          'snowdepth.cast(DecimalType(14, 4)), 'rejectReason)

      actualSuccessDf.collect() should contain theSameElementsAs expectedSuccessDf.collect()
      actualErrorDf.collect() should contain theSameElementsAs expectedErrorDf.collect()
    }



  }

}

  object WeatherDataReusableFunctionsTest {

    System.setProperty("hadoop.home.dir", """C:\Work\winutil\""")

    val testSparkSession = SparkSession
      .builder()
      .appName("app-nyc-taxi-analyzer-test")
      .config("spark.master", "local[1]")
      .getOrCreate()

    val testSparkContext = testSparkSession.sparkContext

    val weatherDataReusableFunctions = new WeatherDataReusableFunctions(testSparkSession)
    val reusableFunctions = new ReusableFunctions(testSparkSession)


}
