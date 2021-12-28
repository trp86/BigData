"""This module is responsible for transform (E) in ETL."""
from pyspark.sql import SparkSession, DataFrame


def extract_file(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Extract the local csv file into a DF.

    Args:
        spark (SparkSession): spark session to read the file
        file_path (str): file_path to extract

    Returns:
        DataFrame of a csv file where the header is the schema

    """
    return spark.read.option("header",True).csv(file_path)
