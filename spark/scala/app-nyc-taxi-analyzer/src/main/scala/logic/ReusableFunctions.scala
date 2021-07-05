package logic


import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import JobConfiguration._

object ReusableFunctions extends SparkConfigs {

  /**
   *
   * @param path Path to file location
   * @return Dataframe
   */
  def createDataFrameFromCsvFiles(path: String): DataFrame = {
    sparkSession.read.format("csv").option("header", true).option("delimiter", ",").load(path)
  }

  /**
   *
   * @param expectedColumnsList
   * @param actualColumnsList
   * @return True if header matches else false
   */
  def isHeaderMatch(expectedColumnsList: List[String], actualColumnsList: List[String]): Boolean = {
    !expectedColumnsList.map(actualColumnsList.contains(_)).contains(false)
  }


  /**
   *
   * @param inputDF
   * @param columnNames
   * @return Success Dataframe , Error Dataframe
   */
  def filterRecordsHavingNegativeValue(inputDF: DataFrame, columnNames: List[String]): (DataFrame, DataFrame) = {

    // Create empty dataframe with schema present in input dataframe along with additional column rejectReason
    val emptyDF = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], inputDF.schema).withColumn("rejectReason", lit(""))

    // Create Error dataframe with reject reason
    val errorDF = columnNames.foldLeft(emptyDF)((df, colName) => {
      df.union(
        inputDF.filter(col(colName) < 0).withColumn("rejectReason", lit(colName + " is negative"))
      )
    })

    // Create Success dataframe
    val successDF = columnNames.foldLeft(inputDF)((df, colName) => {
      df.filter(!(col(colName) < 0))
    })

    (successDF, errorDF)

  }


  /**
   *
   * @param inputDF
   * @param columnNames
   * @return Success Dataframe , Error Dataframe
   */
  def filterRecordsHavingImproperDateTimeValue(inputDF: DataFrame, columnNames: List[String]): (DataFrame, DataFrame) = {

    // Create empty dataframe with schema present in input dataframe along with additional column rejectReason
    val emptyDF = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], inputDF.schema).withColumn("rejectReason", lit(""))

    // Create Success dataframe
    val successDF = columnNames.foldLeft(inputDF)((df, colName) => {
      // Create a new column "timestamp_typecasted" in dataframe after typecasting
      val dfWithDateTimeTypeCasted = df.withColumn("timestamp_typecasted", to_timestamp(col(colName), "yyyy-MM-dd HH:mm:ss"))
      // Only records "timestamp_typecasted" is not null should be considered
      dfWithDateTimeTypeCasted.filter(col("timestamp_typecasted").isNotNull).drop(col("timestamp_typecasted"))
    })

    // Create Error dataframe
    val errorDF = columnNames.foldLeft(emptyDF)((df, colName) => {
      // Create a new column "timestamp_typecasted" in dataframe after typecasting
      val dfWithDateTimeTypeCasted = inputDF.withColumn("timestamp_typecasted", to_timestamp(col(colName), dateTimeFormat))
      // Only records "timestamp_typecasted" is null should be considered as error records
      df.union(
        dfWithDateTimeTypeCasted.filter(col("timestamp_typecasted").isNull).drop(col("timestamp_typecasted")).withColumn("rejectReason", lit(colName + " datetime format is incorrect"))
      )
    })

    (successDF, errorDF)
  }


}
