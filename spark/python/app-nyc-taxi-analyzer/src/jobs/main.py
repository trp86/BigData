"""This module is the entry-point for the run.py to handle spark session \
building and ETL."""

import contextlib
import configparser
from pyspark.sql import SparkSession, functions as f
from pathlib import Path
from typing import Generator
from pyspark.sql import DataFrame
from src.jobs import extract, transform,transform_weather_data, transform_trip_data, load
from src.jobs.utils.general import *
from src.jobs.utils.log_utils import Logger


def jobs_main(sparksession: SparkSession, logger: Logger, config_file_path: str) -> None:
    """
    High-level function to perform the ETL job.

    Args:
        sparksession (SparkSession) : spark session to perform ETL job
        logger (Logger) : logger class instance
        file_path (str): path on which the job will be performed

    """
    # Read config file
    config_file = config_file_path + "conf/app-nyc-taxi-analyzer.ini"
    config_dict = read_config_file (config_file_path = config_file)

    # Create dataframe for trip data
    trip_data_file_path = config_file_path + "data/nyc_trip/"
    df_trip = extract.extract_csv_file(sparksession, trip_data_file_path)
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
    df_weather = extract.extract_csv_file(sparksession, weather_data_file_path)
    logger.info("Weather data" f"{weather_data_file_path} extracted to DataFrame")

    # Header Check for weather data
    columns_df_weather = df_weather.columns
    weather_data_expected_header = config_dict['weather.metadata']['expected.header'].split(",")
    weather_data_header_match_status = is_header_match(expected_columns_list=weather_data_expected_header, actual_columns_list=columns_df_weather)
    logger.info("Header Match Status for weather data:" + str(weather_data_header_match_status))

    # Raise exception if there is a mismatch in header for weather data
    if weather_data_header_match_status is bool(False):
        raise IOError("Mismatch in header for weather data.Please verify !!!!!")
 
    # Perform data quality and add additional columns for trip_data
    (df_trip_success, df_trip_error) = transform_trip_data.perform_dq_and_add_additional_columns(df_trip, config_dict, sparksession)

    # Perform data quality and add additional columns for weather_data
    (df_weather_success, df_weather_error) = transform_weather_data.perform_dq_and_add_additional_columns(df_weather, config_dict, sparksession)
    
    # df_trip_success.show(5, truncate=bool(False))
    # df_trip_error.show(5, truncate=bool(False))

    # df_weather_success.show(5, truncate=bool(False))
    # df_weather_error.show(5, truncate=bool(False))

    # Left outer Join df_trip_success and df_weather_success
    # https://stackoverflow.com/questions/55240023/typeerror-dataframe-object-is-not-callable-spark-data-frame
    df_to_persist = df_trip_success.join(df_weather_success, df_trip_success["trip_date"] == df_weather_success["weather_date"], "left_outer")
   
    df_to_persist.show(5, truncate=bool(False))
    
    load.write_to_path(df_to_persist, config_dict['processed.metadata']['processed.data.success.path'])
    load.write_to_path(df_trip_error, config_dict['processed.metadata']['trip.data.error.path'])
    load.write_to_path(df_weather_error, config_dict['processed.metadata']['weather.data.error.path'])
     
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
        sparksession = spark_builder.appName(app_name).getOrCreate()
    else:
        raise NotImplementedError
    try:
        yield sparksession
    finally:
        sparksession.stop()
