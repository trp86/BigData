"""Test the main file."""

from src.jobs.utils import general as gen
import pytest
from pathlib import Path


@pytest.mark.is_header_match
def test_true_condition_is_header_match() -> None:
    """When extract_csv_file function is called then it should create a dataframe."""
    # ASSEMBLE
    expected_columns_list = """vendor_id,start_date""".split(",")
    actual_columns_list = ["vendor_id", "start_date"]

    # ASERT
    header_match_status = gen.is_header_match(
        expected_columns_list, actual_columns_list
    )

    # ACT
    assert bool(True) == header_match_status


@pytest.mark.is_header_match
def test_false_condition_is_header_match() -> None:
    """When is_header_match function is invoked then it should retrun false if header doesnot match."""
    # ASSEMBLE
    expected_columns_list = """vendor_id,start_date""".split(",")
    actual_columns_list = ["vendor_id", "start_datum"]

    # ASERT
    header_match_status = gen.is_header_match(
        expected_columns_list, actual_columns_list
    )

    # ACT
    assert bool(False) == header_match_status


@pytest.mark.read_config_file
def test_true_condition_read_config_file() -> None:
    """When read_config_file function is invoked then it should retrun a config object."""
    # ASSEMBLE
    test_config_file_path = (
        str(Path(__file__).parent.parent) + """/resources/conf/someconfigfile.ini"""
    )
    expected_config = {"header": {"key": "value"}}

    # ASERT
    actual_config = gen.read_config_file(test_config_file_path)

    # ACT
    assert bool(True) == (expected_config == actual_config)


@pytest.mark.read_config_file
def test_throw_exception_if_config_file_not_present() -> None:
    """When read_config_file function is invoked then it should throw exception if config file is not present."""
    # ASSEMBLE
    config_file_path_not_present = (
        str(Path(__file__).parent.parent) + """/resources/inputdata/iamnotpresent.ini"""
    )

    # ASERT
    with pytest.raises(IOError) as execinfo:
        gen.read_config_file(config_file_path_not_present)

    # ACT
    assert (
        str(execinfo.value)
        == "Cannot open configuration file::- " + config_file_path_not_present
    )
