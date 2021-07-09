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
  val tripDataColumns: List[(String, String, String)] = config.properties.getProperty("trip.data.columns").split("""\|""").map(x => {
    val y = x.split(""":""")
    if (y.length == 3) (y(0), y(1), y(2)) else (y(0), y(1), "")
  }).toList


  val tripDataDQcolumnsOrValueCompare = config.properties.getProperty("trip.data.dq.columnsorvalue.compare").split("""\|""").toList

  val inputPathWeatherData = config.properties.getProperty("weather.data.input.path")
  val weatherDataExpectedHeader = config.properties.getProperty("weather.data.expected.header").split(""",""").toList
  val weatherDataColumns: List[(String, String, String)] = config.properties.getProperty("weather.data.columns").split("""\|""").map(x => {
    val y = x.split(""":""")
    if (y.length == 3) (y(0), y(1), y(2)) else (y(0), y(1), "")
  }).toList
  val weatherDataDQnegativeValueCheckColumns = config.properties.getProperty("weather.data.dq.negativevaluecheck.columns").split(""",""").toList
  val weatherDataDQcolumnsOrValueCompare = config.properties.getProperty("weather.data.dq.columnsorvalue.compare").split("""\|""").toList


  // Processed Data location
  val processedDataSuccessPersistPath = config.properties.getProperty("processed.data.success.path")
  val processedDataErrorTripPersistPath = config.properties.getProperty("processed.data.error.trip.path")
  val processedDataErrorWeatherPersistPath = config.properties.getProperty("processed.data.error.weather.path")



  // Column names
  val COL_NAME_TRIP_DATE = "trip_date"
  val COL_NAME_TRIP_HOUR = "trip_hour"
  val COL_NAME_TRIP_DAY_OF_WEEK = "trip_day_of_week"
  val COL_NAME_PICKUP_DATETIME = "pickup_datetime"

  val COL_NAME_TEMPARATURE_CONDITION = "temperature_condition"
  val COL_NAME_AVERAGE_TEMPARATURE = "averagetemperature"

  val COL_NAME_SNOWFALL_CONDITION = "snowfall_condition"
  val COL_NAME_SNOWFALL = "snowfall"

  val COL_NAME_SNOWDEPTH_CONDITION = "snowdepth_condition"
  val COL_NAME_SNOWDEPTH = "snowdepth"

  val COL_NAME_PRECIPITATION = "precipitation"
  val COL_NAME_RAIN_CONDITION = "rain_condition"

}
