package logic

import logic.ReusableFunctions._


object JobProcessor extends SparkConfigs {

  def process(): Unit = {

    // Create dataframe for trip data
    val dfTrip = createDataFrameFromCsvFiles(JobConfiguration.inputPathTripData)

    // Create dataframe for weather data
    val dfWeather = createDataFrameFromCsvFiles(JobConfiguration.inputPathWeatherData)

    // Check Header for trip data
    // TODO if header mismatch move the file to rejected location
    val dfTripHeaderActualColumns = dfTrip.columns.toList
    val isTripDataHeaderMatch = isHeaderMatch(JobConfiguration.tripDataExpectedHeader, dfTripHeaderActualColumns)

    // Check Header for weather data
    // TODO if header mismatch move the file to rejected location
    val dfWeatherHeaderActualColumns = dfWeather.columns.toList
    val isWeatherDataHeaderMatch = isHeaderMatch(JobConfiguration.weatherDataExpectedHeader, dfWeatherHeaderActualColumns)

    // Data Quality check for trip data (Columns should not have negative value)
    val (successDF, errorDF) = filterRecordsHavingNegativeValue(dfTrip, JobConfiguration.tripDataDQnegativeValueCheckColumns)


    successDF.show()
    println(successDF.count)

    errorDF.show()
    println(errorDF.count)



  return Unit
  }

}
