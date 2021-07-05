package logic

import logic.Test.{columnNames, df1, sparkSession}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit}

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
   * @return
   */
   def filterRecordsHavingNegativeValue(inputDF: DataFrame, columnNames: List[String]): (DataFrame, DataFrame) = {

     // Create empty dataframe with schema present in input dataframe along with additional column rejectReason
     val emptyDF = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], inputDF.schema).withColumn("rejectReason", lit(""))

     // Create Error dataframe with reject reason
     val errorDF = columnNames.foldLeft(emptyDF) ((df, colName) => {
       df.union(
         inputDF.filter(col(colName) < 0).withColumn("rejectReason", lit(colName + " is negative"))
       )
     })

     // Create Success dataframe
     val successDF = columnNames.foldLeft(inputDF) ((df, colName) => {
       df.filter(! (col(colName) < 0))
     })

     (successDF, errorDF)

   }



}
