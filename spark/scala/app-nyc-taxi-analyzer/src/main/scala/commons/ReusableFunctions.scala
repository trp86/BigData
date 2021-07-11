package commons

import logic.JobConfiguration.dateTimeFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, expr, lit, to_date, to_timestamp}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.util.{Failure, Success, Try}

class ReusableFunctions (val sparkSession: SparkSession)  {

  val log = Logger.getLogger("ReusableFunctions")

  /**
   *
   * @param path Path to file location
   * @return Dataframe
   */
  def createDataFrameFromCsvFiles(path: String): DataFrame = {
    Try(sparkSession.read.format("csv").option("header", true).option("delimiter", ",").load(path)) match {
      case Success(df) => df
      case Failure(exception) => {
        if (sparkSession == null) {
          log.error ("Spark Session object is null.")
          throw new Exception("Spark Session object is null.")
        }
        else {
          throw exception
        }

      }
    }
  }

  /**
   *
   * @param expectedColumnsList
   * @param actualColumnsList
   * @return True if header matches else false
   */
  def isHeaderMatch(expectedColumnsList: List[String], actualColumnsList: List[String]): Boolean = {
    val trimmedExpectedColumnsList = expectedColumnsList.map(x => x.trim)
    val trimmedActualColumnsList = actualColumnsList.map(x => x.trim)

    !trimmedExpectedColumnsList.map(trimmedActualColumnsList.contains(_)).contains(false)
  }

  /**
   *
   * @param inputDF
   * @param columnDetails
   * @return Dataframe with typecasted columns
   */
  def typecastColumns (inputDF: DataFrame, columnDetails: List[(String, String, String)]): DataFrame = {
    Try (columnDetails.foldLeft(inputDF) ((df, colDetail) => {
      val (colName, colDataType, otherDetails) = (colDetail._1, colDetail._2, colDetail._3)
      colDataType match {
        case "int" => df.withColumn(colName, df(colName).cast(IntegerType))
        case "double" => df.withColumn(colName, df(colName).cast(DoubleType))
        case "decimal" => df.withColumn(colName, df(colName).cast(colDataType + otherDetails))
        case "datetime" => df.withColumn(colName, df(colName).cast(TimestampType))
        case "date" => df.withColumn(colName, to_date(col(colName), otherDetails))
        case "string" => df.withColumn(colName, df(colName).cast(StringType))
        case _ => throw new Exception ("Unsupported data type for typecasting " + colDataType)
      }
    })) match {
      case Success(df) => df
      case Failure(exception) => {
            log.error(exception.getMessage)
            throw exception
          }
        }
  }


  /**
   *
   * @param inputDF
   * @param columnNames
   * @return Success Dataframe , Error Dataframe
   */
  def filterRecordsHavingNegativeValue(inputDF: DataFrame, columnNames: List[String]): (DataFrame, DataFrame) = {

    // Check if column exists in dataframe. If not present then throw exception
    checkIfColumnsExistInDataFrame(inputDF, columnNames)

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

    // Check if column exists in dataframe. If not present then throw exception
    checkIfColumnsExistInDataFrame(inputDF, columnNames)

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


  /**
   *
   * @param inputDF
   * @param columnNamesToBeComparedWithConstantvalue
   * @param compareOperator
   * @return Success Dataframe , Error Dataframe
   */

  def dataframeColumnsCompare(inputDF: DataFrame, compareExpressions: List[String]): (DataFrame, DataFrame) = {
    // Success dataframe
    val successDF = compareExpressions.foldLeft(inputDF) ((df, compareExpression) => {
      val exprSequence = compareExpression.split(""" """).toSeq
      val (colName, compareOperator, valueOrColTobeCompared) = (exprSequence (0).trim, exprSequence (1).trim, exprSequence (2).trim)

      // Check if column exists in dataframe. If not present then throw exception
      checkIfColumnsExistInDataFrame(inputDF, List(colName))

      Try(valueOrColTobeCompared.toInt) match {
        case Success(value) => value // Some value to be compared Do Nothing
        case Failure(exception) => checkIfColumnsExistInDataFrame(inputDF, List(valueOrColTobeCompared)) // Some column for comparison. check if exists in dataframe
      }

      val colDataType = df.schema(colName).dataType.typeName

      // For String datatypes compare operator can be only (=, !=)
      if (colDataType.equalsIgnoreCase("string") && (compareOperator != "=" || compareOperator != "!=" )) {
        log.error("For string datatype compare operator could only be = and !=")
        throw new Exception("For string datatype compare operator could only be = and !=")
      }

      // create compare expression
      val compareExpr = compareOperator.trim match {
        case "=" | "!=" | ">" | "<" | ">=" | "<=" => colName.trim + " " + compareOperator.trim + " " + valueOrColTobeCompared
        case _ => {
          log.error ("Invalid comparison operator!!!!")
          throw new Exception ("Invalid comparison operator!!!!")
        }
      }
      df.filter(expr(compareExpr))
    }
    )

    // Error dataframe
    // Create empty dataframe with schema present in input dataframe along with additional column rejectReason
    val emptyDF = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], inputDF.schema).withColumn("rejectReason", lit(""))

    val errorDF = compareExpressions.foldLeft(emptyDF) ((df, compareExpression) => {
      val exprSequence = compareExpression.split(""" """).toSeq
      val (colName, compareOperator, valueOrColTobeCompared) = (exprSequence (0).trim, exprSequence (1).trim, exprSequence (2).trim)
      val colDataType = df.schema(colName).dataType.typeName

      // For String datatypes compare operator can be only (=, !=)
      if (colDataType.equalsIgnoreCase("string") && (compareOperator != "=" || compareOperator != "!=" )) {
        log.error("For string datatype compare operator could only be = and !=")
        throw new Exception("For string datatype compare operator could only be = and !=")
      }

      // create compare expression
      val compareExpr = compareOperator.trim match {
        case "=" | "!=" | ">" | "<" | ">=" | "<=" => colName.trim + " " + compareOperator.trim + " " + valueOrColTobeCompared
        case _ => {
          log.error ("Invalid comparison operator!!!!")
          throw new Exception ("Invalid comparison operator!!!!")
        }
      }
      df.union(
        inputDF.filter(!expr(compareExpr)).withColumn("rejectReason", lit(colName + " is not " + compareOperator + " " + valueOrColTobeCompared))
      )
    }
    )

    (successDF, errorDF)

  }


  /**
   *
   * @param inputDF
   * @param columnList
   * @return Unit
   */
  def checkIfColumnsExistInDataFrame(inputDF: DataFrame, columnList: List[String]): Unit = {
    val columnsPresentInDf = inputDF.columns.toList
    val columnsNotPresentInDf = columnList.filter(x => !columnsPresentInDf.contains(x))
    if (columnsNotPresentInDf.size > 0) {
      throw new Exception (columnsNotPresentInDf.mkString(",") + " not present in dataframe.")
    }
  }

  /**
   *
   * @param inputDF
   * @param columnName
   * @return DataFrame
   */
  def renameColumnInDataFrame(inputDF: DataFrame, oldColumnName: String, newColumnName: String): DataFrame = {

    // Check if column exists
    checkIfColumnsExistInDataFrame (inputDF, List(oldColumnName))

    Try(inputDF.withColumnRenamed(oldColumnName, newColumnName)) match {
      case Success(df) => df
      case Failure(exception) => {
        log.error("Exception occured while renaming column in dataframe")
        throw exception
      }
    }
  }

}
