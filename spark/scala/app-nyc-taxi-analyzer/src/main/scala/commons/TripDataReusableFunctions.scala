package commons

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, dayofweek, hour, to_date}
import logic.JobConfiguration._

import scala.util.{Failure, Success, Try}


class TripDataReusableFunctions (val sparkSession: SparkSession)   {

   val log = Logger.getLogger("TripDataReusableFunctions")
   val reusableFunctions = new ReusableFunctions(sparkSession)

  /**
   *
   * @param inputDF
   * @return
   */
  def performDQandAddColumns(inputDF: DataFrame): (DataFrame, DataFrame) = {

    // Data Quality check for trip data (Columns should not have negative value)
    val (successDFNegativeValueCheck, errorDFNegativeValueCheck) = reusableFunctions.filterRecordsHavingNegativeValue(inputDF, tripDataDQnegativeValueCheckColumns)

    // Data Quality check for trip data (Columns should have proper datetime format)
    val (successDFDateTimeColumnCheck, errorDFDateTimeColumnCheck) = reusableFunctions.filterRecordsHavingImproperDateTimeValue(successDFNegativeValueCheck, tripDataDQdateTimeStampFormatCheckColumns)

    // Assign data types to columns
    val dfWithTypecastedColumns = reusableFunctions.typecastColumns(successDFDateTimeColumnCheck, tripDataColumns)

    // Data Quality check for columns to be compared with certain value or any column in dataframe
    val (successDFwithColumnsOrValueCompare, errorDFwithColumnsOrValueCompare) = reusableFunctions.dataframeColumnsCompare(dfWithTypecastedColumns, tripDataDQcolumnsOrValueCompare)

    // Add trip_date column
    val dfWithAdditionalColumns = addAdditionalColumns(successDFwithColumnsOrValueCompare)

    // Create error dataframe
    val errorDF = errorDFNegativeValueCheck.union(errorDFDateTimeColumnCheck).union(errorDFwithColumnsOrValueCompare)

    (dfWithAdditionalColumns, errorDF)

  }


  /**
   *
   * @param inputDF
   * @return DataFrame
   */
  private def addTripDateColumn(inputDF: DataFrame): DataFrame = {

    // Check if column pickup_datetime exists in dataframe
    reusableFunctions.checkIfColumnsExistInDataFrame(inputDF, List (COL_NAME_PICKUP_DATETIME))
    Try(inputDF.withColumn(COL_NAME_TRIP_DATE, to_date(col(COL_NAME_PICKUP_DATETIME), "MM/dd/yyyy"))) match
      {
      case Success(df) => df
      case Failure(exception: Exception) => {
        log.error("Exception occured while calculating " + COL_NAME_TRIP_DATE)
        throw exception
      }
    }
  }

  /**
   *
   * @param inputDF
   * @return DataFrame
   */
  private def addTripHourOfDayColumn(inputDF: DataFrame): DataFrame = {

    // Check if column pickup_datetime exists in dataframe
    reusableFunctions.checkIfColumnsExistInDataFrame(inputDF, List (COL_NAME_PICKUP_DATETIME))
    Try(inputDF.withColumn(COL_NAME_TRIP_HOUR, hour(col(COL_NAME_PICKUP_DATETIME)))) match
    {
      case Success(df) => df
      case Failure(exception: Exception) => {
        log.error("Exception occured while calculating " + COL_NAME_TRIP_HOUR)
        throw exception
      }
    }
  }


  /**
   *
   * @param inputDF
   * @return DataFrame
   */
  private def addTripDayOfWeekColumn(inputDF: DataFrame): DataFrame = {

    // Check if column pickup_datetime exists in dataframe
    reusableFunctions.checkIfColumnsExistInDataFrame(inputDF, List (COL_NAME_PICKUP_DATETIME))

    Try(inputDF.withColumn(COL_NAME_TRIP_DAY_OF_WEEK, dayofweek(col(COL_NAME_PICKUP_DATETIME)))) match
    {
      case Success(df) => df
      case Failure(exception: Exception) => {
        log.error("Exception occured while calculating " + COL_NAME_TRIP_DAY_OF_WEEK)
        throw exception
      }
    }
  }


  /**
   *
   * @param inputDF
   * @return DataFrame
   */
  private def addAdditionalColumns(inputDF: DataFrame): DataFrame = addTripDayOfWeekColumn(addTripHourOfDayColumn(addTripDateColumn(inputDF)))

}
