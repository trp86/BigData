from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.logger import Log4j

def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(fld, fmt))

"""
Added code from line 17 to line 21 for below exception:
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
    spark = SparkSession.builder.master("local[3]").appName("RowDemo").getOrCreate()

    logger = Log4j (spark)

   #Schema
    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())])

    #Row objects
    my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    my_df.printSchema()
    my_df.show()
    #Function to add a column
    new_df = to_date_df(my_df, "M/d/y", "EventDate")
    new_df.printSchema()
    new_df.show()