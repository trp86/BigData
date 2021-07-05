package logic

import logic.JobConfiguration.config
import org.apache.log4j.{Logger, PropertyConfigurator}

import java.util.Properties

class JobConfiguration (propertiesFile: String){

  val log = Logger.getLogger(JobConfiguration.getClass)

  // Read properties file and create config object
  val url = getClass.getResource(propertiesFile)
  val properties: Properties = new Properties()
  properties.load(getClass().getResourceAsStream(propertiesFile))
  PropertyConfigurator.configure(properties)

}

object JobConfiguration {
  val config = new JobConfiguration("""/conf/app-nyc-taxi-analyzer.properties""")
  val dateTimeFormat = config.properties.getProperty("data.datetime.format")

  val inputPathTripData = config.properties.getProperty("trip.data.input.path")
  val tripDataExpectedHeader = config.properties.getProperty("trip.data.expected.header").split(""",""").toList
  val tripDataDQnegativeValueCheckColumns = config.properties.getProperty("trip.data.dq.negativevaluecheck.columns").split(""",""").toList
  val tripDataDQdateTimeStampFormatCheckColumns = config.properties.getProperty("trip.data.dq.datetimestampformatcheck.columns").split(""",""").toList
  val tripDataColumns: List[(String, String)] = config.properties.getProperty("trip.data.columns").split(""",""").map(x => {
    val y = x.split(""":""")
    (y(0), y(1))
  }).toList


  val tripDataDQcolumnsOrValueCompare = config.properties.getProperty("trip.data.dq.columnsorvalue.compare").split("""\|""").toList

  val inputPathWeatherData = config.properties.getProperty("weather.data.input.path")
  val weatherDataExpectedHeader = config.properties.getProperty("weather.data.expected.header").split(""",""").toList
  val weatherDataColumns: List[(String, String)] = config.properties.getProperty("weather.data.columns").split(""",""").map(x => {
    val y = x.split(""":""")
    (y(0), y(1))
  }).toList

  val processedData: String = config.properties.getProperty("processed.data.path")


  // Column names
  val COL_NAME_TRIP_DATE = "trip_date"
  val COL_NAME_TRIP_HOUR = "trip_hour"
  val COL_NAME_TRIP_DAY_OF_WEEK = "trip_day_of_week"
  val COL_NAME_PICKUP_DATETIME = "pickup_datetime"

}
