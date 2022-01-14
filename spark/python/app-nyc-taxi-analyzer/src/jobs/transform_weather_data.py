"""This module is responsible for transform (T) in ETL.This module only contains functions very specific \
to transformation of weather data."""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.jobs import transform 

def perform_dq_and_add_additional_columns(df: DataFrame, config_dict: dict, sparksession: SparkSession) -> tuple : 

    # Change the date column to weather_date as date is a reserved keyword in
    df_weather_renamed = transform.rename_column_in_df(df, "date", "weather_date")

    # Replace with negligible value (0.0001)
    weather_data_column_details = list(map(lambda x: x.split(":"), config_dict['weather.metadata']['columns'].split("|")))
    df_weather_with_T_values_replaced = transform.replace_T_with_negligible_values(df_weather_renamed, weather_data_column_details)

    # Typecast columns for weather data
    weather_data_typecasted = transform.typecastcolumns(df_weather_with_T_values_replaced, weather_data_column_details)

    # Data Quality check for weather data (Columns should not have negative value)
    weather_data_negative_check_columns =  config_dict['weather.metadata']['dq.negativevaluecheck.columns'].split(",")
    (success_df_negative_value_check, error_df_negative_value_check) = transform.filter_records_having_negative_value(sparksession= sparksession, df = weather_data_typecasted, column_names = weather_data_negative_check_columns)

    # Data Quality check for columns to be compared with certain value or any column in dataframe
    weather_data_columnsorvalue_check_columns =  config_dict['weather.metadata']['dq.columnsorvalue.compare'].split("|")
    (success_df_columnsorvalue_check, error_df_columnsorvalue_check) = transform.df_columns_compare(sparksession= sparksession, df = success_df_negative_value_check, compare_expressions = weather_data_columnsorvalue_check_columns)

    # Add additional columns
    success_df = add_rain_condition_column(add_snowdepth_condition_column(add_snowfall_condition_column(add_temperature_condition_column(success_df_columnsorvalue_check))))
    error_df = error_df_negative_value_check.union(error_df_columnsorvalue_check)

    return (success_df, error_df)


def add_temperature_condition_column (df: DataFrame) -> DataFrame :
    """
    Adds the temperature_condition column in the dataframe if averagetemperature column exists

    Args:
        df (DataFrame): Spark DataFrame to which temperature_condition column needs to be added
    """
    # Check if column exists in dataframe. If not then raise error
    transform.check_if_column_exists_in_df (df, ["averagetemperature"])

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
    transform.check_if_column_exists_in_df (df, ["snowfall"])

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
    transform.check_if_column_exists_in_df (df, ["snowdepth"])

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
    transform.check_if_column_exists_in_df (df, ["precipitation"])

    return df.withColumn("rain_condition", when(col("precipitation") <= 0 , "norain")
                                        .when((col("precipitation") > 0) & (col("precipitation") < 0.3), "moderate")
                                        .when((col("precipitation") >= 0.3) & (col("precipitation") < 2), "heavy")
                                        .otherwise("violent"))