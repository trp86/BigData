import sys
from collections import namedtuple
from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4j

if __name__ == "__main__":
    # Spark Conf
    conf = SparkConf().setMaster("local[3]").setAppName("ProcessRDD")

    # Spark Session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Spark Context
    sc = spark.sparkContext
    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: ProcessRDD <filename>")
        sys.exit(-1)


    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)

    # Create namedTuple . schema for the data
    SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])

    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1], cols[2], cols[3], cols[4])))
    filteredRDD = selectRDD.filter(lambda r: r.age < 40)
    kvRDD = filteredRDD.map(lambda r: (r.country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2:v1 + v2)

    colsList = countRDD.collect()
    for x in colsList:
        logger.info(x)

    spark.stop()