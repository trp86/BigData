"""Test the main file."""

from src.jobs.utils import general as gen 
import pytest
from pathlib import Path


@pytest.mark.is_header_match
def test_true_condition_is_header_match():
    # ASSEMBLE
    expected_columns_list = """vendor_id,start_date""".split(",")
    actual_columns_list = ["vendor_id", "start_date"]

    # ASERT
    header_match_status = gen.is_header_match(expected_columns_list, actual_columns_list)

    # ACT
    assert bool(True) == header_match_status

@pytest.mark.is_header_match
def test_false_condition_is_header_match():
    # ASSEMBLE
    expected_columns_list = """vendor_id,start_date""".split(",")
    actual_columns_list = ["vendor_id", "start_datum"]

    # ASERT
    header_match_status = gen.is_header_match(expected_columns_list, actual_columns_list)

    # ACT
    assert bool(False) == header_match_status    

@pytest.mark.read_config_file
def test_true_condition_read_config_file():
    # ASSEMBLE
    test_config_file_path = str(Path(__file__).parent.parent) + """/resources/conf/someconfigfile.ini"""
    expected_config = {"header": {"key": "value"}}

    # ASERT
    actual_config = gen.read_config_file(test_config_file_path)

    # ACT
    assert bool(True) == (expected_config == actual_config)  

@pytest.mark.read_config_file
def test_throw_exception_if_config_file_not_present():
    # ASSEMBLE
    config_file_path_not_present = str(Path(__file__).parent.parent) + """/resources/inputdata/iamnotpresent.ini"""

    # ASERT
    with pytest.raises(IOError) as execinfo: 
         gen.read_config_file(config_file_path_not_present)
    
    # ACT
    assert str(execinfo.value) == "Cannot open configuration file::- " + config_file_path_not_present