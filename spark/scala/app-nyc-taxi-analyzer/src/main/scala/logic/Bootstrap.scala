package logic

import commons.SparkConfigs
import org.apache.log4j.{Logger, PropertyConfigurator}

object Bootstrap extends App with SparkConfigs {

  val log = Logger.getLogger(Bootstrap.getClass)
  PropertyConfigurator.configure("src/main/resources/conf/log4j.properties")

  log.info("Spark session object created successfully.")

  JobProcessor.process()
}
