"""Integration-test transform jobs."""

from pyspark.sql import SparkSession
from src.jobs.main import jobs_main
import pytest
from pathlib import Path
from src.jobs.utils.general import *
from src.jobs.utils import log_utils
import pandas as pd
import glob
import os

@pytest.mark.jobs_main
def test_jobs_main(spark_session_test: SparkSession) -> None:

    # ASSEMBLE
    env: EnvEnum = EnvEnum.dev
    config_file_path = str(Path(__file__).parent.parent) + """/resources/conf/app-nyc-taxi-analyzer.ini"""
    logger = log_utils.Logger(env=env, spark=spark_session_test)
    expected_df_error_trip = pd.read_csv(str(Path(__file__).parent.parent) + """/resources/data/expected_output/error_trip.csv""")
    expected_df_weather_trip = pd.read_csv(str(Path(__file__).parent.parent) + """/resources/data/expected_output/error_weather.csv""")
    expected_df_join_weather_trip = pd.read_csv(str(Path(__file__).parent.parent) + """/resources/data/expected_output/success_join_trip_and_weather_data.csv""")
    
    # ACT
    jobs_main(spark_session_test, logger, config_file_path)

    # ASSERT
    actual_df_error_trip = pd.read_csv(glob.glob(os.path.join(str(Path(__file__).parent.parent) + '/resources/data/output/error/trip/', "*.csv")) [0])
    actual_df_weather_trip = pd.read_csv(glob.glob(os.path.join(str(Path(__file__).parent.parent) + '/resources/data/output/error/weather/', "*.csv")) [0])
    actual_df_join_weather_trip = pd.read_csv(glob.glob(os.path.join(str(Path(__file__).parent.parent) + '/resources/data/output/success/', "*.csv")) [0])

    pd.testing.assert_frame_equal(left=expected_df_error_trip,right=actual_df_error_trip, check_exact=True )
    pd.testing.assert_frame_equal(left=expected_df_weather_trip,right=actual_df_weather_trip, check_exact=True )
    pd.testing.assert_frame_equal(left=expected_df_join_weather_trip,right=actual_df_join_weather_trip, check_exact=True )