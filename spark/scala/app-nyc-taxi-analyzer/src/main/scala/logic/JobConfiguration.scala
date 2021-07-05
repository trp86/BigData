package logic

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

  val inputPathTripData = config.properties.getProperty("trip.data.input.path")
  val tripDataExpectedHeader = config.properties.getProperty("trip.data.expected.header").split(""",""").toList
  val tripDataDQnegativeValueCheckColumns = config.properties.getProperty("trip.data.dq.negativevaluecheck.columns").split(""",""").toList

  val inputPathWeatherData = config.properties.getProperty("weather.data.input.path")
  val weatherDataExpectedHeader = config.properties.getProperty("weather.data.expected.header").split(""",""").toList

  val processedData: String = config.properties.getProperty("processed.data.path")
}
