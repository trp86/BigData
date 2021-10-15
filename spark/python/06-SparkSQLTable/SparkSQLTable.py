from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":

    spark = SparkSession.builder.master("local[3]").appName("SparkSQLTable").enableHiveSupport ().getOrCreate()

    logger = Log4j (spark)

    flightTimeParquetDF = spark.read.format ("parquet").load ("data/")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTimeParquetDF.write.mode ("overwrite").saveAsTable ("flight_data_tbl")

    logger.info (spark.catalog.listTables ("AIRLINE_DB"))