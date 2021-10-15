import sys
from collections import namedtuple
from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4j

if __name__ == "__main__":

    # Spark Session
    spark = SparkSession.builder.master("local[3]").appName("SparkSQL").getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: SparkSQL <filename>")
        sys.exit(-1)

    surveyDF = spark.read.option("header", "true").option("inferSchema", "true").csv(sys.argv[1])
    surveyDF.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql("select Country,count(1) as count from survey_tbl where Age < 40 group by Country")

    countDF.show()

    spark.stop()