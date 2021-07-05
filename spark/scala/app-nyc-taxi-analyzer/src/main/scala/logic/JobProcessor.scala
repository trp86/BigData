package logic

import JobConfiguration._
import commons.TripDataReusableFunctions._
import commons.WeatherDataReusableFunctions.replaceTwithNegligibleValues


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

    /* val (dfTripSuccess, dfTripError) = performDataQualityAndAddAdditionalColumns(dfTrip)

    println(dfTripSuccess.count)
    println(dfTripError.count)

    dfTripSuccess.show()
    dfTripError.show()
*/

    dfWeather.printSchema()

    val x = replaceTwithNegligibleValues(dfWeather, weatherDataColumns)


    val y = typecastColumns(x, weatherDataColumns)
    y.show(400)
    y.printSchema()

  return Unit
  }

}
