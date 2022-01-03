"""Testcases for transform.py file."""

from os import error
from src.jobs.utils.general import LibCommons
import pytest
from pyspark.sql import SparkSession
from pathlib import Path
from src.jobs import transform 

@pytest.fixture
def init():
    app_name = "test_" + Path(__file__).parent.name
    test_spark_session = SparkSession.builder.appName(app_name).getOrCreate()
    libCommons = LibCommons(sparkSession=test_spark_session)
    return libCommons, test_spark_session

@pytest.mark.check_if_column_exists_in_df
def test_column_list_to_be_checked_return_none_if_column_exist(init):
    # given
    test_spark_session = init [1]
    some_df = test_spark_session.createDataFrame([(1234,56.76), (2341,76.45)], schema='vendor_id string, total_amount string')
    column_list_to_be_checked = ["vendor_id", "total_amount"]
    
    # when
    actual = transform.check_if_column_exists_in_df(some_df, column_list_to_be_checked)
    
    # then
    assert actual is None


@pytest.mark.check_if_column_exists_in_df
def test_column_list_to_raise_error_if_column_not_exist(init):
    # given
    test_spark_session = init [1]
    some_df = test_spark_session.createDataFrame([(1234,56.76), (2341,76.45)], schema='vendor_id string, total_amount string')
    column_list_to_be_checked = ["vendor_id", "total_amount", "trip_date"]
    expected_columns_not_present_in_df = ["trip_date"]


    # when
    with pytest.raises(IOError) as execinfo:
       transform.check_if_column_exists_in_df(some_df, column_list_to_be_checked)

    # then
    assert str(execinfo.value) == 'Columns not present in dataframe::- ' + str(expected_columns_not_present_in_df)