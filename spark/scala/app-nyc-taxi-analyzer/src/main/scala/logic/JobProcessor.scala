package logic

import JobConfiguration._
import commons.TripDataReusableFunctions._
import commons.{TripDataReusableFunctions, WeatherDataReusableFunctions}


object JobProcessor  {

  def process(): Unit = {

    // Create dataframe for trip data
    val dfTrip = createDataFrameFromCsvFiles(inputPathTripData)

    // Create dataframe for weather data
    val dfWeather = createDataFrameFromCsvFiles(inputPathWeatherData).withColumnRenamed("date", "weather_date")

    // Check Header for trip data
    // TODO if header mismatch move the file to rejected location
    val dfTripHeaderActualColumns = dfTrip.columns.toList
    val isTripDataHeaderMatch = isHeaderMatch(tripDataExpectedHeader, dfTripHeaderActualColumns)

    // Check Header for weather data
    // TODO if header mismatch move the file to rejected location
    val dfWeatherHeaderActualColumns = dfWeather.columns.toList
    val isWeatherDataHeaderMatch = isHeaderMatch(weatherDataExpectedHeader, dfWeatherHeaderActualColumns)

     val (dfTripSuccess, dfTripError) = TripDataReusableFunctions.performDQandAddColumns(dfTrip)

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

    val (dfWeatherSuccess, dfWeatherError) = WeatherDataReusableFunctions.performDQandAddColumns(dfWeather)
    // Join dfTripSuccess and dfWeatherSuccess
    val df1 = dfTripSuccess.join(dfWeatherSuccess, dfTripSuccess("trip_date") === dfWeatherSuccess("weather_date"), "inner")

    // println (df1.count)

     // df1.show(false)

    df1.write.option("header", true).csv("src/main/resources/inputdata/nyccitytaxi/out/")






    return Unit
  }

}
