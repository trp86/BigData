"""General utilities around the ETL job."""
from enum import Enum
from typing import List
from pyspark.sql import SparkSession
import configparser

class EnvEnum(Enum):
    """Maintain the enums of the environments."""

    dev = "dev"
    prod = "prod"
    stage = "stage"


class LibCommons:
    """Common Reusable Functions"""
    def __init__(
        self,
        sparkSession: SparkSession,
    ) -> None:
        """
        Initialize the LibCommons class

        Args:
            spark (SparkSession): 
        """
        self.sparkSession = sparkSession

    def is_header_match(self, expected_columns_list: List, actual_columns_list: List) -> bool:
        """
        Checks if header list is matching or not. 

        Args:
        expected_columns_list (List): Column Names to be expected
        actual_columns_list (List): Actual Column Names

        Returns:
        Boolean True if header matches else False

        """
        # Trims the extra spaces
        trim_expected_columns_list = list(map(lambda column: column.strip(), expected_columns_list))
        trim_actual_columns_list = list(map(lambda column: column.strip(), actual_columns_list))
        
        #  Creates a list of Boolean values if a actual column name is present in expected column list
        header_match = list(map(lambda column: column in trim_actual_columns_list, trim_expected_columns_list))

        #  Boolean True if header matches else False
        return bool(False) if (bool(False) in header_match) else bool(True)

    def read_config_file(self, config_file_path: str) -> dict:
        """
        Reads a config file and returns a python dict 

        Args:
        config_file_path (str): Config File Path

        Returns:
        Dict with config parameters

        """    
        config = configparser.RawConfigParser()
        if config.read(config_file_path) == []:
            raise IOError('Cannot open configuration file::- ' + config_file_path)
        else:     
            # config._sections converts configparser object to python dict
            return config._sections 