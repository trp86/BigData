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
    data_file_path = config_file_path + "data/"
    df_trip = extract.extract_csv_file(spark, data_file_path)
    logger.info("Trip data " f"{data_file_path} extracted to DataFrame")

    # Header Check for trip data
    actual_header = df_trip.columns
    expected_header = config_dict['trip.metadata']['expected.header'].split(",")
    header_match_status = libCommons.is_header_match(expected_columns_list=expected_header, actual_columns_list=actual_header)
    logger.info("Header Match Status for trip data:" + str(header_match_status))

    # Create dataframe for weather data
    # df_trip = extract.extract_csv_file(spark, file_path)
    # logger.info("Trip data" f"{file_path} extracted to DataFrame")

     
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
