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

if __name__ == "__main__":

    #Spark Session Object
    spark = SparkSession.builder.master("local[3]").appName("Spark Transformations").getOrCreate()

    logger = Log4j (spark)

    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981

    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
    raw_df.printSchema()

    final_df = raw_df.withColumn("id", monotonically_increasing_id()) \
        .withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("year", col("year").cast(IntegerType())) \
        .withColumn("year", when(col("year") < 20, col("year") + 2000)
                    .when(col("year") < 100, col("year") + 1900)
                    .otherwise(col("year"))) \
        .withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')")) \
        .drop("day", "month", "year") \
        .dropDuplicates(["name", "dob"]).sort(col("dob").desc())
        # .sort(expr("dob desc")) This doesn't seem to be working


final_df.show()