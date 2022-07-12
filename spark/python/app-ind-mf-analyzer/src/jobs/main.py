"""This module is the entry-point for the run.py to handle spark session \
building and ETL."""

import contextlib
from pyspark.sql import SparkSession
from pathlib import Path
from typing import Generator

from src.jobs.utils.general import is_header_match, read_config_file, EnvEnum
from src.jobs.utils.log_utils import Logger


def jobs_main(
    sparksession: SparkSession, logger: Logger, config_file_path: str
) -> None:
    """
    High-level function to perform the ETL job.

    Args:
        sparksession (SparkSession) : spark session to perform ETL job
        logger (Logger) : logger class instance
        config_file_path (str): config file name along with full path

    """
    # Read config file
    config_dict = read_config_file(config_file_path)

    print("Its working!!!!")

    


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
