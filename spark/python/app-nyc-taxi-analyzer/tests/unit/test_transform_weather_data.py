"""Testcases for transform_weather_data.py file."""

import pytest
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from src.jobs.utils.general import *
import pytest
from pyspark.sql import SparkSession
from src.jobs import transform_weather_data
from pyspark.sql.types import *
from pyspark.sql import Row 
import pandas as pd
from pyspark.sql.functions import col

@pytest.mark.add_temperature_condition_column
# when add_temperature_condition_column function is invoked it should add temperature_condition  column to input dataframe
def test_add_temperature_condition_column_if_averagetemperature_column_exists (spark_session_test: SparkSession):
    # ASSEMBLE
    input_df = spark_session_test.createDataFrame([
        Row(averagetemperature=-22), Row(averagetemperature=28), Row(averagetemperature=32), Row(averagetemperature=44), 
        Row(averagetemperature=59), Row(averagetemperature=62), Row(averagetemperature=77), Row(averagetemperature=84), 
        Row(averagetemperature=95), Row(averagetemperature=105)]) \
        .withColumn("averagetemperature",col("averagetemperature").cast(IntegerType()))

    expected_df = spark_session_test.createDataFrame([
        Row(averagetemperature=-22, temperature_condition="verycold"), 
        Row(averagetemperature=28, temperature_condition="verycold"), 
        Row(averagetemperature=32, temperature_condition="cold"), 
        Row(averagetemperature=44, temperature_condition="cold"), 
        Row(averagetemperature=59, temperature_condition="normal"),
        Row(averagetemperature=62, temperature_condition="normal"), 
        Row(averagetemperature=77, temperature_condition="hot"), 
        Row(averagetemperature=84, temperature_condition="hot"), 
        Row(averagetemperature=95, temperature_condition="veryhot"), 
        Row(averagetemperature=105, temperature_condition="veryhot")]) \
        .withColumn("averagetemperature",col("averagetemperature").cast(IntegerType()))

    # ACT
    actual_df = transform_weather_data.add_temperature_condition_column(input_df)
    
    # ASSERT
    pd.testing.assert_frame_equal(left=expected_df.toPandas(),right=actual_df.orderBy(['averagetemperature', 'temperature_condition'], ascending=True).toPandas(),check_exact=True )


@pytest.mark.add_temperature_condition_column
# when add_temperature_condition_column function is invoked it should throw exception if averagetemperature column is not present in dataframe
def test_add_temperature_condition_raise_exception_if_averagetemperature_column_not_present(spark_session_test: SparkSession):
    # ASSEMBLE
    input_df = spark_session_test.createDataFrame([
        Row(avgtemp=-22)]) \
        .withColumn("avgtemp",col("avgtemp").cast(IntegerType()))

     # ACT
    with pytest.raises(Exception) as execinfo:
       transform_weather_data.add_temperature_condition_column(input_df)

    # ASSERT
    assert str(execinfo.value) == """Columns not present in dataframe::- ['averagetemperature']"""

@pytest.mark.add_snowfall_condition_column
# when add_snowfall_condition_column function is invoked it should add snowfall column to input dataframe
def test_add_snowfall_condition_column_if_snowfall_column_exists (spark_session_test: SparkSession):
    # ASSEMBLE
    input_df = spark_session_test.createDataFrame([
        Row(snowfall="-2"), Row(snowfall="0.0001"), Row(snowfall="0.7"), Row(snowfall="2"), 
        Row(snowfall="4"), Row(snowfall="10"), Row(snowfall="15"), Row(snowfall="30")]) \
        .withColumn("snowfall",col("snowfall").cast(DecimalType(14, 4)))

    expected_df = spark_session_test.createDataFrame([
        Row(snowfall="-2", snowfall_condition="nosnow"), 
        Row(snowfall="0.0001", snowfall_condition="nosnow"), 
        Row(snowfall="0.7", snowfall_condition="moderate"), 
        Row(snowfall="2", snowfall_condition="moderate"), 
        Row(snowfall="4", snowfall_condition="heavy"), 
        Row(snowfall="10", snowfall_condition="heavy"), 
        Row(snowfall="15", snowfall_condition="violent"), 
        Row(snowfall="30", snowfall_condition="violent")]) \
        .withColumn("snowfall",col("snowfall").cast(DecimalType(14, 4))) \
        .orderBy(['snowfall', 'snowfall_condition'], ascending=True)

    # ACT
    actual_df = transform_weather_data.add_snowfall_condition_column(input_df)
    
    # ASSERT
    pd.testing.assert_frame_equal(left=expected_df.toPandas(),right=actual_df.orderBy(['snowfall', 'snowfall_condition'], ascending=True).toPandas(),check_exact=True )


@pytest.mark.add_snowfall_condition_column
# when add_snowfall_condition_column function is invoked it should throw exception if snowfall column is not present in dataframe
def test_add_snowfall_condition_column_raise_exception_if_snowfall_column_not_present(spark_session_test: SparkSession):
    # ASSEMBLE
    input_df = spark_session_test.createDataFrame([
        Row(snow_fall="-2")]) \
        .withColumn("snow_fall",col("snow_fall").cast(DecimalType(14, 4)))

     # ACT
    with pytest.raises(Exception) as execinfo:
       transform_weather_data.add_snowfall_condition_column(input_df)

    # ASSERT
    assert str(execinfo.value) == """Columns not present in dataframe::- ['snowfall']"""