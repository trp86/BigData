package logic

import JobConfiguration._
import commons.{ReusableFunctions, TripDataReusableFunctions, WeatherDataReusableFunctions}
import org.apache.spark.sql.SparkSession


object JobProcessor  {

  def process(sparkSession: SparkSession): Unit = {

    val reusableFunctions = new ReusableFunctions(sparkSession)
    // import reusableFunctions._

    val tripDataReusableFunctions = new TripDataReusableFunctions(sparkSession)
    // import tripDataReusableFunctions._

    val weatherDataReusableFunctions = new WeatherDataReusableFunctions(sparkSession)
    // import weatherDataReusableFunctions._



    // Create dataframe for trip data
    val dfTrip = reusableFunctions.createDataFrameFromCsvFiles(inputPathTripData)

    // Create dataframe for weather data
    val dfWeather = reusableFunctions.createDataFrameFromCsvFiles(inputPathWeatherData).withColumnRenamed("date", "weather_date")

    // Check Header for trip data
    // TODO if header mismatch move the file to rejected location
    val dfTripHeaderActualColumns = dfTrip.columns.toList
    val isTripDataHeaderMatch = reusableFunctions.isHeaderMatch(tripDataExpectedHeader, dfTripHeaderActualColumns)

    // Check Header for weather data
    // TODO if header mismatch move the file to rejected location
    val dfWeatherHeaderActualColumns = dfWeather.columns.toList
    val isWeatherDataHeaderMatch = reusableFunctions.isHeaderMatch(weatherDataExpectedHeader, dfWeatherHeaderActualColumns)

     val (dfTripSuccess, dfTripError) = tripDataReusableFunctions.performDQandAddColumns(dfTrip)

    /* println(dfTripSuccess.count)
    println(dfTripError.count)

    dfTripSuccess.show()
    dfTripError.show()

    val (dfWeatherSuccess, dfWeatherError) = WeatherDataReusableFunctions.performDQandAddColumns(dfWeather)

    println(dfWeatherSuccess.count)
    println(dfWeatherError.count)

    dfWeatherSuccess.show()
    dfWeatherError.show()
*/

    val (dfWeatherSuccess, dfWeatherError) = weatherDataReusableFunctions.performDQandAddColumns(dfWeather)
    // Join dfTripSuccess and dfWeatherSuccess
    val df1 = dfTripSuccess.join(dfWeatherSuccess, dfTripSuccess("trip_date") === dfWeatherSuccess("weather_date"), "inner")

    // println (df1.count)

      df1.show(false)

    // df1.write.option("header", true).csv("src/main/resources/inputdata/nyccitytaxi/out/")






    return Unit
  }

}
