"""This module is responsible for transform (T) in ETL.This module only contains functions very specific \
to transformation of weather data."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.jobs.transform import check_if_column_exists_in_df


def add_additional_columns(df: DataFrame) -> DataFrame : 
    return add_rain_condition_column(add_snowdepth_condition_column(add_snowfall_condition_column(add_temperature_condition_column(df))))


def add_temperature_condition_column (df: DataFrame) -> DataFrame :
    """
    Adds the temperature_condition column in the dataframe if averagetemperature column exists

    Args:
        df (DataFrame): Spark DataFrame to which temperature_condition column needs to be added
    """
    # Check if column exists in dataframe. If not then raise error
    check_if_column_exists_in_df (df, ["averagetemperature"])

    df.printSchema()
    # https://stackoverflow.com/questions/65732120/pyspark-py4j-py4jexception-method-andclass-java-lang-integer-does-not-exist
    return df.withColumn("temperature_condition", when(col("averagetemperature") < 32, "verycold")
                                                .when((col("averagetemperature") >= 32) & (col("averagetemperature") < 59), "cold")
                                                .when((col("averagetemperature") >= 59) & (col("averagetemperature") < 77), "normal")
                                                .when((col("averagetemperature") >= 77) & (col("averagetemperature") < 95), "hot")
                                                .otherwise("veryhot"))

def add_snowfall_condition_column (df: DataFrame) -> DataFrame :
    """
    Adds the snowfall_condition column in the dataframe if snowfall column exists

    Args:
        df (DataFrame): Spark DataFrame to which snowfall_condition column needs to be added
    """
    # Check if column exists in dataframe. If not then raise error
    check_if_column_exists_in_df (df, ["snowfall"])

    return df.withColumn("snowfall_condition", when(col("snowfall") <= 0.0001 , "nosnow")
                                            .when((col("snowfall") >= 0.0001) & (col("snowfall") < 4), "moderate")
                                            .when((col("snowfall") >= 4) & (col("snowfall") < 15), "heavy")
                                            .otherwise("violent"))

def add_snowdepth_condition_column (df: DataFrame) -> DataFrame :
    """
    Adds the snowdepth_condition column in the dataframe if snowdepth column exists

    Args:
        df (DataFrame): Spark DataFrame to which snowdepth_condition column needs to be added
    """
    # Check if column exists in dataframe. If not then raise error
    check_if_column_exists_in_df (df, ["snowdepth"])

    return df.withColumn("snowdepth_condition", when(col("snowdepth") <= 0.0001 , "nosnow")
                                            .when((col("snowdepth") >= 0.0001) & (col("snowdepth") < 4), "moderate")
                                            .when((col("snowdepth") >= 4) & (col("snowdepth") < 15), "heavy")
                                            .otherwise("violent"))

def add_rain_condition_column (df: DataFrame) -> DataFrame :
    """
    Adds the rain_condition column in the dataframe if precipitation column exists

    Args:
        df (DataFrame): Spark DataFrame to which snowdepth_condition column needs to be added
    """
    # Check if column exists in dataframe. If not then raise error
    check_if_column_exists_in_df (df, ["precipitation"])

    return df.withColumn("rain_condition", when(col("precipitation") <= 0 , "norain")
                                        .when((col("precipitation") > 0) & (col("precipitation") < 0.3), "moderate")
                                        .when((col("precipitation") >= 0.3) & (col("precipitation") < 2), "heavy")
                                        .otherwise("violent"))