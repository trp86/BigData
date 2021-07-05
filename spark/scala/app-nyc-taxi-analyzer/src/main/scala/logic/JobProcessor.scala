package logic

import logic.ReusableFunctions._
import JobConfiguration._

object JobProcessor extends SparkConfigs {

  def process(): Unit = {

    // Create dataframe for trip data
    val dfTrip = createDataFrameFromCsvFiles(inputPathTripData)

    // Create dataframe for weather data
    val dfWeather = createDataFrameFromCsvFiles(inputPathWeatherData)

    // Check Header for trip data
    // TODO if header mismatch move the file to rejected location
    val dfTripHeaderActualColumns = dfTrip.columns.toList
    val isTripDataHeaderMatch = isHeaderMatch(tripDataExpectedHeader, dfTripHeaderActualColumns)

    // Check Header for weather data
    // TODO if header mismatch move the file to rejected location
    val dfWeatherHeaderActualColumns = dfWeather.columns.toList
    val isWeatherDataHeaderMatch = isHeaderMatch(weatherDataExpectedHeader, dfWeatherHeaderActualColumns)

    // Data Quality check for trip data (Columns should not have negative value)
    val (successDFNegativeValueCheck, errorDFNegativeValueCheck) = filterRecordsHavingNegativeValue(dfTrip, tripDataDQnegativeValueCheckColumns)

    // Data Quality check for trip data (Columns should have proper datetime format)
    val (successDFDateTimeColumnCheck, errorDFDateTimeColumnCheck) = filterRecordsHavingImproperDateTimeValue(successDFNegativeValueCheck, tripDataDQdateTimeStampFormatCheckColumns)

    // Assign data types to columns
    val dfWithTypecastedColumns = typecastColumns(successDFDateTimeColumnCheck, tripDataColumns)



    println(tripDataDQcolumnsOrValueCompare)
    val (successDFwithPassengerCountCheck, errorDFwithPassengerCountCheck) = dataframeColumnsCompare(dfWithTypecastedColumns, tripDataDQcolumnsOrValueCompare)

    successDFwithPassengerCountCheck.show()
    println(successDFwithPassengerCountCheck.count)

    val errorDF = errorDFNegativeValueCheck.union(errorDFDateTimeColumnCheck).union(errorDFwithPassengerCountCheck)
    println(errorDF.count)
    errorDF.show(truncate = false)



  return Unit
  }

}
