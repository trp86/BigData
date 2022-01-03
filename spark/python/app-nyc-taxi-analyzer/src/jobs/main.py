"""This module is the entry-point for the run.py to handle spark session \
building and ETL."""

import contextlib
import configparser
from pyspark.sql import SparkSession
from pathlib import Path
from typing import Generator

from src.jobs import extract, transform, load
from src.jobs.utils.general import EnvEnum
from src.jobs.utils.general import LibCommons
from src.jobs.utils.log_utils import Logger


def jobs_main(spark: SparkSession, logger: Logger, config_file_path: str, libCommons: LibCommons) -> None:
    """
    High-level function to perform the ETL job.

    Args:
        spark (SparkSession) : spark session to perform ETL job
        logger (Logger) : logger class instance
        file_path (str): path on which the job will be performed

    """
    # Read config file
    config_file = config_file_path + "conf/app-nyc-taxi-analyzer.ini"
    config_dict = libCommons.read_config_file (config_file)

    # Create dataframe for trip data
    trip_data_file_path = config_file_path + "data/nyc_trip/"
    df_trip = extract.extract_csv_file(spark, trip_data_file_path)
    logger.info("Trip data " f"{trip_data_file_path} extracted to DataFrame")

    # Header Check for trip data
    columns_df_trip = df_trip.columns
    trip_data_expected_header = config_dict['trip.metadata']['expected.header'].split(",")
    trip_data_header_match_status = libCommons.is_header_match(expected_columns_list=trip_data_expected_header, actual_columns_list=columns_df_trip)
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
    weather_data_header_match_status = libCommons.is_header_match(expected_columns_list=weather_data_expected_header, actual_columns_list=columns_df_weather)
    logger.info("Header Match Status for weather data:" + str(weather_data_header_match_status))

    # Raise exception if there is a mismatch in header for weather data
    if weather_data_header_match_status is bool(False):
        raise IOError("Mismatch in header for weather data.Please verify !!!!!")


    df_weather.show(20)
    df_trip.show(20)    
     
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
