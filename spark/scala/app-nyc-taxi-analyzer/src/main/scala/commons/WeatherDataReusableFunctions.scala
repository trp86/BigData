package commons

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}

object WeatherDataReusableFunctions extends ReusableFunctions {

  override val log = Logger.getLogger("WeatherDataReusableFunctions")

  /**
   *
   * @param inputDf
   * @param columnDetails
   * @return DataFrame
   */
  def replaceTwithNegligibleValues(inputDf: DataFrame, columnDetails: List[(String, String)]): DataFrame = {
    columnDetails.foldLeft(inputDf) ((df, colDetail) => {
      val colName = colDetail._1
      val colDataType = colDetail._2
      colDataType match {
        case "double" => df.withColumn(colName, when(col(colName).equalTo("T"), "0.0001").otherwise(col(colName)))
        case _ => df
      }
    })
  }
}
