package logic

import JobConfiguration._
import commons.{ReusableFunctions, TripDataReusableFunctions, WeatherDataReusableFunctions}
import org.apache.spark.sql.{SaveMode, SparkSession}

object JobProcessor  {

  def process(sparkSession: SparkSession): Unit = {

    val reusableFunctions = new ReusableFunctions(sparkSession)

    val tripDataReusableFunctions = new TripDataReusableFunctions(sparkSession)

    val weatherDataReusableFunctions = new WeatherDataReusableFunctions(sparkSession)

    // Create dataframe for trip data
    val dfTrip = reusableFunctions.createDataFrameFromCsvFiles(inputPathTripData)

    // Create dataframe for weather data and rename column date to weather_date
    val dfWeather = reusableFunctions.createDataFrameFromCsvFiles(inputPathWeatherData) // reusableFunctions.renameColumnInDataFrame(reusableFunctions.createDataFrameFromCsvFiles(inputPathWeatherData), "date", "weather_date")

    // Check Header for trip data
    val dfTripHeaderActualColumns = dfTrip.columns.toList
    val isTripDataHeaderMatch = reusableFunctions.isHeaderMatch(tripDataExpectedHeader, dfTripHeaderActualColumns)

    // Check Header for weather data
    val dfWeatherHeaderActualColumns = dfWeather.columns.toList
    val isWeatherDataHeaderMatch = reusableFunctions.isHeaderMatch(weatherDataExpectedHeader, dfWeatherHeaderActualColumns)

    // If header match fails throw exception and stop job execution
    if (isTripDataHeaderMatch == false ) throw new Exception ("Trip data header doesnot match.")
    if (isWeatherDataHeaderMatch == false ) throw new Exception ("Weather data header doesnot match.")

    // Change the date column to weather_date as date is a reserved keyword
    val dfWeatherRenamed = reusableFunctions.renameColumnInDataFrame(dfWeather, "date", "weather_date")

    // Perform data quality and filter out success and error records
     val (dfTripSuccess, dfTripError) = tripDataReusableFunctions.performDQandAddColumns(dfTrip)
     val (dfWeatherSuccess, dfWeatherError) = weatherDataReusableFunctions.performDQandAddColumns(dfWeatherRenamed)

    // Join dfTripSuccess and dfWeatherSuccess
    val dfToPersist = dfTripSuccess.join(dfWeatherSuccess, dfTripSuccess("trip_date") === dfWeatherSuccess("weather_date"), "left_outer")

    // Store the dataframe in partitioned way
    dfToPersist.write.partitionBy("weather_date").mode(SaveMode.Overwrite).save(processedDataSuccessPersistPath)
    dfTripError.write.partitionBy("rejectReason").mode(SaveMode.Overwrite).save(processedDataErrorTripPersistPath)
    dfWeatherError.write.partitionBy("rejectReason").mode(SaveMode.Overwrite).save(processedDataErrorWeatherPersistPath)

    // TODO Add install.md,changelog.md and readme.md
    // TODO Add jacaco plugin
    // TODO Check why tests are not running when mvn test is executed

  }

}
