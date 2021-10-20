from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.logger import Log4j
from pyspark.sql import functions as f

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
    spark = SparkSession.builder.master("local[3]").appName("SparkAggregations").getOrCreate()

    logger = Log4j (spark)

    summary_df = spark.read.parquet("data/summary.parquet")

    running_total_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(-2, Window.currentRow)

    summary_df.withColumn("RunningTotal",
                          f.sum("InvoiceValue").over(running_total_window)) \
        .show()

    summary_df.sort("Country", "WeekNumber").show()

    rank_window = Window.partitionBy("Country") \
        .orderBy(f.col("InvoiceValue").desc()) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary_df.withColumn("Rank", f.dense_rank().over(rank_window)) \
        .where(f.col("Rank") == 1) \
        .sort("Country", "WeekNumber") \
        .show()

