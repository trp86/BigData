"""This module is responsible for transform (E) in ETL."""
from pyspark.sql import SparkSession, DataFrame
import sys
from os import path

def extract_csv_file(sparksession: SparkSession, file_path: str) -> DataFrame:
    """
    Extract the csv files into a DF.

    Args:
        sparksession (SparkSession): spark session to read the file
        file_path (str): file_path to extract

    Returns:
        DataFrame of a csv file where the header is the schema

    """
    try:
        return sparksession.read.option("header",True).csv(file_path)
    
    except AttributeError as ae:
        if sparksession is None:
            raise IOError ("Spark Session object is None!!!!")

    except:
        # Check if file_path exists
        if path.exists(file_path) is bool(False):
             raise IOError('Path desnot exist::- ' + file_path)


