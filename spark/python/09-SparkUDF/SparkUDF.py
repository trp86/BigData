from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.logger import Log4j
import re

"""
Added code from line 15 to line 19 for below exception:
Exception: Python in worker has different version 3.8 than that in driver 3.9, PySpark cannot run with different minor versions.
Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.

https://stackoverflow.com/questions/48260412/environment-variables-pyspark-python-and-pyspark-driver-python 
"""

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"



if __name__ == "__main__":

    #Spark Session Object
    spark = SparkSession.builder.master("local[3]").appName("SparkUDF").getOrCreate()

    logger = Log4j (spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/survey.csv")

    survey_df.show(10)

    parse_gender_udf = udf(parse_gender, returnType=StringType())
    logger.info("Catalog Entry:")
    [logger.info(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]

    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    logger.info("Catalog Entry:")
    [logger.info(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]

    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(10)