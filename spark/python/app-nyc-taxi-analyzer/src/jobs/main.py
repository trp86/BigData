"""This module is the entry-point for the run.py to handle spark session \
building and ETL."""

import contextlib
import configparser
from pyspark.sql import SparkSession
from pathlib import Path
from typing import Generator

from src.jobs import extract, transform, load
from src.jobs.utils.general import *
from src.jobs.utils.log_utils import Logger


def jobs_main(spark: SparkSession, logger: Logger, config_file_path: str) -> None:
    """
    High-level function to perform the ETL job.

    Args:
        spark (SparkSession) : spark session to perform ETL job
        logger (Logger) : logger class instance
        file_path (str): path on which the job will be performed

    """
    # Read config file
    config_file = config_file_path + "conf/app-nyc-taxi-analyzer.ini"
    config_dict = read_config_file (config_file_path = config_file)

    # Create dataframe for trip data
    trip_data_file_path = config_file_path + "data/nyc_trip/"
    df_trip = extract.extract_csv_file(spark, trip_data_file_path)
    logger.info("Trip data " f"{trip_data_file_path} extracted to DataFrame")

    # Header Check for trip data
    columns_df_trip = df_trip.columns
    trip_data_expected_header = config_dict['trip.metadata']['expected.header'].split(",")
    trip_data_header_match_status = is_header_match(expected_columns_list=trip_data_expected_header, actual_columns_list=columns_df_trip)
    logger.info("Header Match Status for trip data:" + str(trip_data_header_match_status))

    # Raise exception if there is a mismatch in header for trip data
    if trip_data_header_match_status is bool(False):
        raise IOError("Mismatch in header for trip data.Please verify !!!!!")

    # Create dataframe for weather data
    weather_data_file_path = config_file_path + "data/weather/"
    df_weather = extract.extract_csv_file(spark, weather_data_file_path)
    logger.info("Weather data" f"{weather_data_file_path} extracted to DataFrame")

    # Header Check for weather data
    columns_df_weather = df_weather.columns
    weather_data_expected_header = config_dict['weather.metadata']['expected.header'].split(",")
    weather_data_header_match_status = is_header_match(expected_columns_list=weather_data_expected_header, actual_columns_list=columns_df_weather)
    logger.info("Header Match Status for weather data:" + str(weather_data_header_match_status))

    # Raise exception if there is a mismatch in header for weather data
    if weather_data_header_match_status is bool(False):
        raise IOError("Mismatch in header for weather data.Please verify !!!!!")
 
    # Data Quality check for trip data
    
    # Filter records and put to error dataframe which is having a negative value
    trip_data_negative_check_columns =  config_dict['trip.metadata']['dq.negativevaluecheck.columns'].split(",")
    (success_df_negative_value_check, error_df_negative_value_check) = transform.filter_records_having_negative_value(sparksession= spark, df = df_trip, column_names = trip_data_negative_check_columns)

    
    trip_data_datetimestamp_check_columns =  config_dict['trip.metadata']['dq.datetimestampformatcheck.columns'].split(",")
    (success_df_datetime_check, error_df_datetime_check) = transform.filter_records_having_improper_datetime_value(sparksession= spark, df = success_df_negative_value_check, column_names = trip_data_datetimestamp_check_columns)
    

    # Typecast columns for trip data
    trip_data_column_details = list(map(lambda x: x.split(":"), config_dict['trip.metadata']['columns'].split("|")))
    list(map(lambda a: a==a.insert(2,"") if len(a) == 2 else a , trip_data_column_details))
    trip_data_typecasted = transform.typecastcolumns(success_df_datetime_check, trip_data_column_details)

    trip_data_columnsorvalue_check_columns =  config_dict['trip.metadata']['dq.columnsorvalue.compare'].split("|")
    (success_df_columnsorvalue_check, error_df_columnsorvalue_check) = transform.df_columns_compare(sparksession= spark, df = trip_data_typecasted, compare_expressions = trip_data_columnsorvalue_check_columns)
    
    success_df_columnsorvalue_check.show(truncate = bool(False))
    error_df_columnsorvalue_check.show(truncate = bool(False))

    quit()
    
    # Change the date column to weather_date as date is a reserved keyword in
    df_weather_renamed = transform.rename_column_in_df(df_weather, "date", "weather_date")

    # Typecast columns for weather data
    weather_data_column_details = list(map(lambda x: x.split(":"), config_dict['weather.metadata']['columns'].split("|")))
    list(map(lambda a: a==a.insert(2,"") if len(a) == 2 else a , weather_data_column_details))

    weather_data_typecasted = transform.typecastcolumns(df_weather_renamed, weather_data_column_details)
    #weather_data_typecasted.printSchema()

   

   # df_trip.printSchema()

   # t = transform.filter_records_having_negative_value(sparksession= spark, df = df_trip, column_names = ["passenger_count", "trip_distance"])

 #   t [0].show(2)
   # t [1].show(20)
    # df_weather_renamed.show(20)
    # df_trip.show(20)    
     
    """count_df = transform.transform_df(df)
    logger.info("Counted words in the DataFrame")

    load.write_to_path(count_df)
    logger.info("Written counted words to path")"""


@contextlib.contextmanager
def spark_build(env: EnvEnum) -> Generator[SparkSession, None, None]:
    """
    Build the spark object.

    Args:
        env (EnvEnum): environment of the spark-application

    Yields:
        SparkSession object

    """
    spark_builder = SparkSession.builder
    app_name = Path(__file__).parent.name

    if env == EnvEnum.dev:
        spark = spark_builder.appName(app_name).getOrCreate()
    else:
        raise NotImplementedError
    try:
        yield spark
    finally:
        spark.stop()
