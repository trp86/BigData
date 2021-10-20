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

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    invoice_df.select(f.count("*").alias("Count *"),
                      f.sum("Quantity").alias("TotalQuantity"),
                      f.avg("UnitPrice").alias("AvgPrice"),
                      f.countDistinct("InvoiceNo").alias("CountDistinct")
                      ).show()

    invoice_df.selectExpr(
        "count(1) as `count 1`",
        "count(StockCode) as `count field`",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice"
    ).show()

    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
             SELECT Country, InvoiceNo,
                   sum(Quantity) as TotalQuantity,
                   round(sum(Quantity*UnitPrice),2) as InvoiceValue
             FROM sales
             GROUP BY Country, InvoiceNo""")

    summary_sql.show()

    summary_df = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr")
             )

    summary_df.show()

    NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    exSummary_df = invoice_df \
        .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
        .groupBy("Country", "WeekNumber") \
        .agg(NumInvoices, TotalQuantity, InvoiceValue)

    exSummary_df.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("output")

    exSummary_df.sort("Country", "WeekNumber").show()

