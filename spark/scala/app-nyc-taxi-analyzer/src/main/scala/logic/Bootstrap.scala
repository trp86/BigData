package logic

import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.sql.SparkSession

object Bootstrap extends App with SparkConfigs {

  val log = Logger.getLogger(Bootstrap.getClass)
  PropertyConfigurator.configure("src/main/resources/conf/log4j.properties")

  log.info("Spark session object created successfully.")

  JobProcessor.process()
}
