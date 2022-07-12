"""Testcases for extract.py file."""

import pytest
from pathlib import Path
from src.jobs import extract
from pyspark.sql import SparkSession


@pytest.mark.extract_csv_file
def test_true_condition_extract_csv_file(spark_session_test: SparkSession) -> None:
    """When extract_csv_file function is called then it should create a dataframe."""
    # ASSEMBLE
    test_extract_csv_file_location = (
        str(Path(__file__).parent.parent) + """/resources/data/input/test_extract/"""
    )
    expected_df = spark_session_test.createDataFrame(
        [(1234, 56.76), (2341, 76.45), (1009, 12), (120, 75)],
        schema="vendor_id string, total_amount string",
    )

    # ACT
    actual_df = extract.extract_csv_file(
        spark_session_test, test_extract_csv_file_location
    )

    # ASSERT
    assert (
        expected_df is not None
    ), "Expected a dataframe of data to be returned from function"
    assert (
        expected_df.collect() == actual_df.collect()
    ), "Expected DF and Actual DF Should match"
    assert actual_df.count() == 4, "Expected DF and Actual DF count Should match"
    assert (
        expected_df.schema == actual_df.schema
    ), "Expected DF and Actual DF schema Should match"


@pytest.mark.extract_csv_file
def test_exception_if_sparksession_object_is_null() -> None:
    """When extract_csv_file function is called then it should throw exception if spark session object is null."""
    # ASSEMBLE
    test_extract_csv_file_location = (
        Path(__file__).parent.parent.name + "/resources/input/test_extract/"
    )

    # ACT
    with pytest.raises(IOError) as execinfo:
        extract.extract_csv_file(None, test_extract_csv_file_location)

    # ASSERT
    assert str(execinfo.value) == "Spark Session object is None!!!!"


@pytest.mark.extract_csv_file
def test_exception_if_file_path_doesnot_exist(spark_session_test: SparkSession) -> None:
    """When extract_csv_file function is called then it should throw exception if csv file path is invalid."""
    # ASSEMBLE
    test_extract_csv_file_location = (
        Path(__file__).parent.parent.name + "/resources/input/not_exist/"
    )

    # ACT
    with pytest.raises(IOError) as execinfo:
        extract.extract_csv_file(spark_session_test, test_extract_csv_file_location)

    # ASSERT
    assert (
        str(execinfo.value) == "Path desnot exist::- " + test_extract_csv_file_location
    )
