"""Testcases for transform_trip_data.py file."""

import pytest
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from src.jobs.utils.general import *
import pytest
from pyspark.sql import SparkSession
from src.jobs import transform_trip_data
from pyspark.sql.types import *
from pyspark.sql import Row 
import pandas as pd
from pyspark.sql.functions import col

@pytest.mark.add_trip_date_column
# when addTripDateColumn function is invoked it should add trip_date column to input dataframe
def test_add_trip_date_column_if_pickup_datetime_column_exists(spark_session_test: SparkSession):
    # ASSEMBLE
    input_df = spark_session_test.createDataFrame([
        Row(vendor_id="CMT", pickup_datetime="2014-01-09 20:45:25"), 
        Row(vendor_id="CMT", pickup_datetime="2014-01-09 22:45:25")]) \
        .withColumn("pickup_datetime",col("pickup_datetime").cast(TimestampType()))

    expected_df = spark_session_test.createDataFrame([
        Row(vendor_id="CMT", pickup_datetime="2014-01-09 20:45:25", trip_date="2014-01-09"), 
        Row(vendor_id="CMT", pickup_datetime="2014-01-09 22:45:25", trip_date="2014-01-09")]) \
        .withColumn("pickup_datetime",col("pickup_datetime").cast(TimestampType()))  \
        .withColumn("trip_date",col("trip_date").cast(DateType()))  \
        .orderBy(['vendor_id', 'pickup_datetime', 'trip_date'], ascending=True)

    # ACT
    actual_df = transform_trip_data.add_trip_date_column(input_df)
    
    # ASSERT
    pd.testing.assert_frame_equal(left=expected_df.toPandas(),right=actual_df.orderBy(['vendor_id', 'pickup_datetime', 'trip_date'], ascending=True).toPandas(),check_exact=True )


@pytest.mark.add_trip_date_column
# when addTripDateColumn function is invoked it should throw exception if pickup_datetime column is not present in dataframe
def test_add_trip_date_column_raise_exception_if_pickup_datetime_column_not_present(spark_session_test: SparkSession):
    # ASSEMBLE
    input_df = spark_session_test.createDataFrame([
        Row(vendor_id="CMT", start_datetime="2014-01-09 20:45:25"), 
        Row(vendor_id="CMT", start_datetime="2014-01-09 22:45:25")]) \
        .withColumn("start_datetime",col("start_datetime").cast(TimestampType()))

     # ACT
    with pytest.raises(Exception) as execinfo:
       transform_trip_data.add_trip_date_column(input_df)

    # ASSERT
    assert str(execinfo.value) == """Columns not present in dataframe::- ['pickup_datetime']"""

@pytest.mark.add_trip_hour_column
# when add_trip_hour_column function is invoked it should add trip_hour  column to input dataframe
def test_add_trip_hour_column_if_pickup_datetime_column_exists(spark_session_test: SparkSession):
    # ASSEMBLE
    input_df = spark_session_test.createDataFrame([
        Row(vendor_id="CMT", pickup_datetime="2014-01-09 20:45:25"), 
        Row(vendor_id="CMT", pickup_datetime="2014-01-09 22:45:25")]) \
        .withColumn("pickup_datetime",col("pickup_datetime").cast(TimestampType()))

    expected_df = spark_session_test.createDataFrame([
        Row(vendor_id="CMT", pickup_datetime="2014-01-09 20:45:25", trip_hour=20), 
        Row(vendor_id="CMT", pickup_datetime="2014-01-09 22:45:25", trip_hour=22)]) \
        .withColumn("pickup_datetime",col("pickup_datetime").cast(TimestampType()))  \
        .withColumn("trip_hour",col("trip_hour").cast(IntegerType()))  \
        .orderBy(['vendor_id', 'pickup_datetime', 'trip_hour'], ascending=True)

    # ACT
    actual_df = transform_trip_data.add_trip_hour_column(input_df)
    
    # ASSERT
    pd.testing.assert_frame_equal(left=expected_df.toPandas(),right=actual_df.orderBy(['vendor_id', 'pickup_datetime', 'trip_hour'], ascending=True).toPandas(),check_exact=True )


@pytest.mark.add_trip_hour_column
# when add_trip_hour_column function is invoked it should throw exception if pickup_datetime column is not present in dataframe
def test_add_trip_hour_column_raise_exception_if_pickup_datetime_column_not_present(spark_session_test: SparkSession):
    # ASSEMBLE
    input_df = spark_session_test.createDataFrame([
        Row(vendor_id="CMT", start_datetime="2014-01-09 20:45:25"), 
        Row(vendor_id="CMT", start_datetime="2014-01-09 22:45:25")]) \
        .withColumn("start_datetime",col("start_datetime").cast(TimestampType()))

     # ACT
    with pytest.raises(Exception) as execinfo:
       transform_trip_data.add_trip_date_column(input_df)

    # ASSERT
    assert str(execinfo.value) == """Columns not present in dataframe::- ['pickup_datetime']"""


@pytest.mark.add_trip_day_of_week_column
# when add_trip_day_of_week_column function is invoked it should add trip_day_of_week  column to input dataframe
def test_add_trip_day_of_week_column_if_pickup_datetime_column_exists(spark_session_test: SparkSession):
    # ASSEMBLE
    input_df = spark_session_test.createDataFrame([
        Row(vendor_id="CMT", pickup_datetime="2014-01-09 20:45:25"),
        Row(vendor_id="CMT", pickup_datetime="2014-01-10 22:45:25"), 
        Row(vendor_id="CMT", pickup_datetime="2014-01-11 22:45:25")]) \
        .withColumn("pickup_datetime",col("pickup_datetime").cast(TimestampType()))

    expected_df = spark_session_test.createDataFrame([
        Row(vendor_id="CMT", pickup_datetime="2014-01-09 20:45:25", trip_day_of_week=5),
        Row(vendor_id="CMT", pickup_datetime="2014-01-10 22:45:25", trip_day_of_week=6), 
        Row(vendor_id="CMT", pickup_datetime="2014-01-11 22:45:25", trip_day_of_week=7)]) \
        .withColumn("pickup_datetime",col("pickup_datetime").cast(TimestampType()))  \
        .withColumn("trip_day_of_week",col("trip_day_of_week").cast(IntegerType()))  \
        .orderBy(['vendor_id', 'pickup_datetime', 'trip_day_of_week'], ascending=True)

    # ACT
    actual_df = transform_trip_data.add_trip_day_of_week_column(input_df)
    
    # ASSERT
    pd.testing.assert_frame_equal(left=expected_df.toPandas(),right=actual_df.orderBy(['vendor_id', 'pickup_datetime', 'trip_day_of_week'], ascending=True).toPandas(),check_exact=True )


@pytest.mark.add_trip_day_of_week_column
# when add_trip_day_of_week_column function is invoked it should throw exception if pickup_datetime column is not present in dataframe
def test_add_trip_day_of_week_column_raise_exception_if_pickup_datetime_column_not_present(spark_session_test: SparkSession):
    # ASSEMBLE
    input_df = spark_session_test.createDataFrame([
        Row(vendor_id="CMT", start_datetime="2014-01-09 20:45:25"),
        Row(vendor_id="CMT", start_datetime="2014-01-10 22:45:25"), 
        Row(vendor_id="CMT", start_datetime="2014-01-11 22:45:25")]) \
        .withColumn("start_datetime",col("start_datetime").cast(TimestampType()))

     # ACT
    with pytest.raises(Exception) as execinfo:
       transform_trip_data.add_trip_day_of_week_column(input_df)

    # ASSERT
    assert str(execinfo.value) == """Columns not present in dataframe::- ['pickup_datetime']"""