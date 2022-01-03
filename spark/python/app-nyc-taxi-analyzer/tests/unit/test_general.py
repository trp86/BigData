"""Test the main file."""

from src.jobs.utils.general import LibCommons
import pytest
from pyspark.sql import SparkSession
from pathlib import Path


@pytest.fixture
def init():
    app_name = "test_" + Path(__file__).parent.name
    test_spark_session = SparkSession.builder.appName(app_name).getOrCreate()
    libCommons = LibCommons(sparkSession=test_spark_session)
    return libCommons, test_spark_session

@pytest.mark.is_header_match
def test_true_condition_is_header_match(init):
    # given
    expected_columns_list = """vendor_id,start_date""".split(",")
    actual_columns_list = ["vendor_id", "start_date"]
    lib_commons = init [0] 

    # when
    header_match_status = lib_commons.is_header_match(expected_columns_list, actual_columns_list)

    # then
    assert bool(True) == header_match_status

@pytest.mark.is_header_match
def test_false_condition_is_header_match(init):
    # given
    expected_columns_list = """vendor_id,start_date""".split(",")
    actual_columns_list = ["vendor_id", "start_datum"]
    lib_commons = init [0] 

    # when
    header_match_status = lib_commons.is_header_match(expected_columns_list, actual_columns_list)

    # then
    assert bool(False) == header_match_status    

@pytest.mark.read_config_file
def test_true_condition_read_config_file(init):
    # given
    test_config_file_path = str(Path(__file__).parent.parent) + """/resources/conf/someconfigfile.ini"""
    lib_commons = init [0] 
    expected_config = {"header": {"key": "value"}}

    # when
    actual_config = lib_commons.read_config_file(test_config_file_path)

    # then
    assert bool(True) == (expected_config == actual_config)  

@pytest.mark.read_config_file
def test_throw_exception_if_config_file_not_present(init):
    # given
    config_file_path_not_present = str(Path(__file__).parent.parent) + """/resources/inputdata/iamnotpresent.ini"""
    lib_commons = init [0]

    # when
    with pytest.raises(IOError) as execinfo: 
         lib_commons.read_config_file(config_file_path_not_present)
    
    # then
    assert str(execinfo.value) == "Cannot open configuration file::- " + config_file_path_not_present


