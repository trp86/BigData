"""Testcases for extract.py file."""

from os import error
from src.jobs.utils.general import LibCommons
import pytest
from pyspark.sql import SparkSession
from pathlib import Path
from src.jobs import extract 
import pathlib

@pytest.fixture
def init():
    app_name = "test_" + Path(__file__).parent.name
    test_spark_session = SparkSession.builder.appName(app_name).getOrCreate()
    libCommons = LibCommons(sparkSession=test_spark_session)
    return libCommons, test_spark_session

@pytest.mark.extract_csv_file
def test_true_condition_extract_csv_file(init):
    # given
    test_extract_csv_file_location =  str(pathlib.Path().absolute()) + "/resources/input/test_extract/"
    test_spark_session = init [1] 
    expected_df = test_spark_session.createDataFrame([(1234,56.76), (2341,76.45), (1009,12), (120,75)], schema='vendor_id string, total_amount string')

    # when
    actual_df = extract.extract_csv_file(test_spark_session, test_extract_csv_file_location)

    # then
    assert expected_df is not None, 'Expected a dataframe of data to be returned from function'
    assert expected_df.collect() == actual_df.collect(), 'Expected DF and Actual DF Should match'
    assert actual_df.count() == 4, 'Expected DF and Actual DF count Should match' 
    assert expected_df.schema == actual_df.schema, 'Expected DF and Actual DF schema Should match'

@pytest.mark.extract_csv_file
def test_exception_if_sparksession_object_is_null(init):
    # given
    test_extract_csv_file_location =  Path(__file__).parent.parent.name + "/resources/input/test_extract/"
    test_spark_session = None 

    # when
    with pytest.raises(IOError) as execinfo:
        extract.extract_csv_file(test_spark_session, test_extract_csv_file_location)

    # then
    assert str(execinfo.value) == "Spark Session object is None!!!!"
    
@pytest.mark.extract_csv_file
def test_exception_if_file_path_doesnot_exist(init):
    # given
    test_extract_csv_file_location =  Path(__file__).parent.parent.name + "/resources/input/not_exist/"
    test_spark_session = init [1] 

    # when
    with pytest.raises(IOError) as execinfo:
        extract.extract_csv_file(test_spark_session, test_extract_csv_file_location)

    # then
    assert str(execinfo.value) == 'Path desnot exist::- ' + test_extract_csv_file_location    