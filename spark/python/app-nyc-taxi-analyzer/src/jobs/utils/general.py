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
            trim_expected_columns_list = list(map(lambda column: column.strip(), expected_columns_list))
            trim_actual_columns_list = list(map(lambda column: column.strip(), actual_columns_list))

            header_match = list(map(lambda column: column in trim_actual_columns_list, trim_expected_columns_list))

            return bool(False) if (bool(False) in header_match) else bool(True)

    def read_config_file(self, property_file_path: str) -> dict:
        config = configparser.RawConfigParser()
        config.read(property_file_path)
        return config        